package agent

import (
	"Proglog/internal/auth"
	"Proglog/internal/discovery"
	"Proglog/internal/log"
	"Proglog/internal/server"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ServerTLSConfig 		*tls.Config
	PeerTLSConfig 			*tls.Config
	DataDir 				string		// existing commit log data
	BindAddr 				string		// This address also contains the port that Serf uses for gossiping
	RPCPort 				int			// port used for RPC
	NodeName 				string		// node name for service discovery
	StartJoinAddrs			[]string	// Addresses to start with when trying to join a cluster
	ACLModelFile 			string
	ACLPolicyFile			string
	BootstrapRaft			bool
}

// Agent sets up the components so its Config comprises the component parameters
// to pass them through to the components
type Agent struct {
	Config
	multiplexer			cmux.CMux
	log					*log.DistributedLog
	server				*grpc.Server
	membership 			*discovery.Membership
	alreadyShutdown		bool
	shutdowns 			chan struct{}
	shutdownLock 		sync.Mutex
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// Creates an Agent and runs a set of methods to set up and run the
// agent's components. After we run New(), we expect to have a running,
// functioning service
func New(config Config) (*Agent, error) {
	a := &Agent {
		Config: 	config,
		shutdowns:	make(chan struct{}),
	}
	setup := []func() error {
		a.setupLogger,
		a.setupMultiplexer,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	go a.serve()

	return a, nil
}

// Tell mux to server connections
func (a *Agent) serve() error {
	if err := a.multiplexer.Serve(); err != nil {
		a.Shutdown()
		return err
	}
	return nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// Configure distributed log's Raft to use our multiplexed listener
// and then configure and create the distributed log
func (a *Agent) setupLog() error {
	raftListener := a.multiplexer.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Equal(b, []byte{byte(log.RaftRPC)})
	})

	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftListener,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	rpcAddr, err := a.Config.RPCAddr()

	if err != nil {
		return err
	}

	logConfig.Raft.BindAddr = rpcAddr
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.BootstrapRaft
	logConfig.Raft.CommitTimeout = 1000 * time.Millisecond
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}
	if a.Config.BootstrapRaft {
		err = a.log.WaitForLeader(3 * time.Second)
	}

	return err
}

func (a *Agent) setupMultiplexer() error {
	addr, err := net.ResolveTCPAddr("tcp", a.Config.BindAddr)
	if err != nil {
		return err
	}
	rpcAddr := fmt.Sprintf(
		"%s:%d",
		addr.IP.String(),
		a.Config.RPCPort,
	)

	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.multiplexer = cmux.New(listener)
	return nil
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	serverConfig := &server.Config {
		CommitLog: 	a.log,
		Authorizer: authorizer,
		GetServerer: a.log,
	}

	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	grpcListener := a.multiplexer.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcListener); err != nil {
			a.Shutdown()
		}
	}()

	return err
}

// Create Membership
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	a.membership, err = discovery.New(
		a.log,
		discovery.Config{
			NodeName: a.Config.NodeName,
			BindAddr: a.Config.BindAddr,
			Tags: map[string]string {
				"rpc_addr": rpcAddr,
			},
			StartJoinAddrs: a.Config.StartJoinAddrs,
		},
	)

	return err
}

// This method ensures the following actions will happen:
// -	Leave the membership so that other servers will see that
// 		this server has left the cluster so that this server does
//		not receive discovery events anymore
// -	Close the replicator so it doesn't continue to replicate
// -	Gracefully stop the server
// -	Close the log
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.alreadyShutdown {
		return nil
	}

	a.alreadyShutdown = true

	close(a.shutdowns)

	shutdown := []func() error {
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fun := range shutdown {
		if err := fun(); err != nil {
			return err
		}
	}
	return nil
}
