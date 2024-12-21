package log

import (
	api "Proglog/api/v1"
	"context"
	"io"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer	api.LogClient
	Logger		*zap.Logger
	mu			sync.Mutex
	servers		map[string]chan struct{}
	closed		bool
	close 		chan struct{}
}

// Adds the given server address to the list of servers to replicate and kicks off
// the add goroutine to run the actual replication logic 
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if (r.servers == nil) {
		r.servers = make(map[string]chan struct{})
	} else if (r.servers[name] != nil) {
		// already replicated
		return nil
	}

	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])
	
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.NewClient(addr, r.DialOptions...)
	
	if err != nil {
		r.logError(err, "Failed to dial", addr)
		return
	}
	defer cc.Close()

	logClient := api.NewLogClient(cc)
	ctx := context.Background()

	remoteStreamingClient, err := logClient.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "Failed to create bidirectional streaming client", addr)
		return
	}

	recordsToReplicate := make(chan *api.Record)

	go func () {
		for {
		response, err := remoteStreamingClient.Recv()
		if err != nil {
			if err != io.EOF {
				r.logError(err, "Unexpected error happened when reading records from remote server", "")
				return
			}
			return
		}
		recordsToReplicate <- response.Record
	}}()

	for {
		select {
		case <- r.close:
			return
		case <- leave:
			return                
		case record := <- recordsToReplicate:
			_, err = r.LocalServer.Produce(
				ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				if err != io.EOF {
					r.logError(err, "Unexpected error happened when produce request to remote server", addr)
				}
				return 
			}
		}
	}

	
}

// This method handles the server leaving the cluster by removing the server from
// the list of servers to replicate and close the server's associated channel
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.servers[name]; !ok {
		return nil
	}

	close(r.servers[name])
	delete(r.servers, name)

	return nil
}

// // lazily initialize the replicator
// func (r *Replicator) init() {
// 	if (r.Logger == nil) {
// 		r.Logger = zap.L().Named("replicator")
// 	}
// 	if (r.servers == nil) {
// 		r.servers = make(map[string]chan struct{})
// 	}
// 	if (r.close == nil) {
// 		r.close = make(chan struct{})
// 	}
// }

// Close the replicator so it does not replciate new servers that join
// the cluster and it stops replicating exisiting servers by causing the
// `replicate()` goroutine to return
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	if (r.close != nil) {
		close(r.close)
	}
	r.closed = true
	return nil
}

func (r * Replicator) logError(err error, errMsg string, addr string) {
	r.Logger.Error(
		errMsg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}