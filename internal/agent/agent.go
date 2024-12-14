package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	api "Proglog/api/v1"
	"Proglog/internal/auth"
	"Proglog/internal/discovery"
	"Proglog/internal/log"
	"Proglog/internal/server"
)

type Config struct {
	ServerTLSConfig 		*tls.Config
	PeerTLSConfig 			*tls.Config
	DataDir 				string
	BindAddr 				string
	RPCPort 				int
	NodeName 				string
	StartJoinAddrs			[]string
	ACLModelFile 			string
	ACLPolicyFile			string
}

// Agent sets up the components so its Config comprises the component parameters
// to pass them through to the components
type Agent struct {
	Config
	log					*log.Log
	server				*grpc.Server
	membership 			*discovery.Membership
	replicator 	 		*log.Replicator
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
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(
		a.Config.DataDir,
		log.Config{},
	)
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	serverConfig := &server.Config {
		CommitLog: 	a.log,
		Authorizer: authorizer,
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
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := a.server.Serve(ln); err != nil {
			a.Shutdown()
		}
	}()

	return err
}

// Sets up a Replicator with gRPC dial options needed to connect to other 
// servers and a client so that the replicator can connect to other servers,
// consume their data, and produce a copy of the data to the local server
//
// Then, we create a Membership using the replicator and its handler to notify
// the replicator when servers join and leave the cluster
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	var opts []grpc.DialOption
	if a.Config.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(a.Config.PeerTLSConfig),
		))
	}
	conn, err := grpc.NewClient(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator {
		DialOptions: opts,
		LocalServer: client,
	}
	a.membership, err = discovery.New(
		a.replicator,
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
		a.replicator.Close,
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
