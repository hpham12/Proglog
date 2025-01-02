package log

import (
	api "Proglog/api/v1"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log		*Log
	raft	*raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (
	*DistributedLog,
	error,
) {
	l := &DistributedLog{
		config: config,
	}

	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

// Creates log for this server, where this server will store the user's records
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

// Method to setup Raft. A Raft instance comprises:
// 1.	A finite state machine that applies the commands you give to Raft
// 2.	A log store where Raft stores those commands
// 3.	A stable store where Raft stores the cluster's config - the servers in the cluster,
//		their addresses, and so on
// 4.	A snapshot store where Raft stores compact snapshots of its data
// 5.	A transport that Raft uses to connect with the server's peers
func (l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logConfig := l.config
	logConfig.Segment.InitialOffset = 1	// Raft requires log initial offset to 1
	logStore, err := newLogStore(logDir, logConfig)

	if err != nil {
		return err
	}
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)

	if err != nil {
		return err
	}
	snapshotRetained := 1

	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		snapshotRetained,
		os.Stderr,
	)

	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second

	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	// LocalID is the unique ID for this server
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)

	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)

	if err != nil {
		return err
	}

	// bootstrap a server configured with itself as the only voter,
	// wait until it becomes the leader, then tell the leader to add
	// more servers to the cluster. The sunsequently added servers
	// don't bootstrap
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration {
			Servers: []raft.Server{{
				ID:			config.LocalID,
				Address:	transport.LocalAddr(),
			}},
		}
		l.raft.BootstrapCluster(config).Error()
	}

	return nil
}

// Appends the record to the log. Instead of directly append the record to the server's log,
// we tell Raft to apply a command that tells the FSM to append the record to the log. Raft
// will replicate the command to a majority of the Raft servers and ultimately append the record
// to a majority of Raft servers
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)

	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

// Wraps Raft's API to apply requests and return their responses. This method can support
// multiple request types
func (l *DistributedLog) apply(requestType RequestType, request proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(requestType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Reads the record for the offset from ther server's log. For better performance, I use 
// eventual consistent read here, which doesn't need to go through Raft
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

func (fsm *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])

	switch reqType {
	case AppendRequestType:
		return fsm.applyAppend(buf[1:])
	}
	return nil
}

func (fsm *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := fsm.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{Offset: offset}
}

func (fsm *fsm) Snapshot() (
	raft.FSMSnapshot,
	error,
) {
	r := fsm.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Raft will call this method on the FSMSnapshot we created to write
// its state to some sink
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Raft will call this method when it is finished with the snapshot
func (s *snapshot) Release() {}

// Raft calls this method to restore an FSM from a snapshot
func (fsm *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(encodingStyle.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}

		record := &api.Record{}
		if err = proto.Unmarshal(b, record); err != nil {
			return err
		}

		if i == 0 {
			fsm.log.Config.Segment.InitialOffset = record.Offset
			if err := fsm.log.Reset(); err != nil {
				return err
			}
		}
		
		if _, err = fsm.log.Append(record); err != nil {
			return err
		}

		buf.Reset()
	}

	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term

	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

// Raft use this API to append records to its log
func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Offset: record.Index,
			Term: record.Term,
			Type: uint32(record.Type),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// Raft uses a stream layer in the transport to provide a low-level
// stream abstraction to connect with Raft servers
type StreamLayer struct {
	listener 			net.Listener
	serverTLSConfig		*tls.Config
	peerTLSConfig		*tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		listener:			ln,
		serverTLSConfig: 	serverTLSConfig,
		peerTLSConfig:		peerTLSConfig,
	}
}

const RaftRPC = 1

// Makes outgoing connections to other servers in the Rat cluster.
// When we connect to a server, we write the RaftRPC byte to identify
// the connection type so we can multiplex Raft on the same port as our
// Log gRPC requests
func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// identify to mux this is a Raft RPC
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, err
}

// Handle incomming connections and read the byte that identifies the
// connection and then create a server-side TLS connection
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.listener.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.listener.Addr()
}

// Adds the server to the Raft cluster
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server has already joined
				return nil
			}
			// remove the exisiting server
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

// Removes the server from the cluster
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// Blocks until the cluster has elected a leader or times out
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <- timeoutc:
			return fmt.Errorf("timed out")
		
		case <- ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}
