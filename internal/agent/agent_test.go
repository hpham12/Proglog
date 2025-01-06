package agent

import (
	api "Proglog/api/v1"
	"Proglog/internal/config"
	"Proglog/internal/loadbalance"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestAgent(t *testing.T) {

	// serverTLSConfig defines the configuration of the certificate that'
	// served to the clients
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:		config.ServerCertFile,
		KeyFile:		config.ServerKeyFile,
		CAFile:			config.CAFile,
		Server:			true,
		ServerAddress:	"127.0.0.1",
	})

	require.NoError(t, err)

	// peerTLSConfig defines the configuration of the certificate that's served
	// between servers so they can connect with and replicate each other
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:		config.RootClientCertFile,
		KeyFile:		config.RootClientKeyFile,
		CAFile:			config.CAFile,
		Server:			false,
		ServerAddress:	"127.0.0.1",
	})

	require.NoError(t, err)

	var agents []*Agent

	for i := 0; i < 3; i++ {
		// get 2 random free ports, one used for gRPC log connection, one for Serf discovery connection
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		agent, err := New(Config{
			BootstrapRaft: 		i == 0,
			NodeName:			fmt.Sprintf("%d", i),
			StartJoinAddrs: 	startJoinAddrs,
			BindAddr:			bindAddr,
			RPCPort: 			rpcPort,
			DataDir:			dataDir,
			ACLModelFile: 		config.ACLModelFile,
			ACLPolicyFile: 		config.ACLPolicyFile,
			ServerTLSConfig: 	serverTLSConfig,
			PeerTLSConfig: 		peerTLSConfig,
		})

		require.NoError(t, err)

		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()

	leaderClient := client(t, agents[0])
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("helloworld"),
			},
		},
	)

	require.NoError(t, err)

	// wait until replication has finished
	time.Sleep(3 * time.Second)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("helloworld"))

	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset + 1,
		},
	)

	require.Nil(t, consumeResponse)
	require.Error(t, err)
	require.Equal(t, status.Code(api.ErrOffsetOutOfRange{}), status.Code(err))
}

func client(t *testing.T, a *Agent) api.LogClient {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(
			credentials.NewTLS(a.Config.PeerTLSConfig),
		),
	}
	rpcAddr, err := a.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.NewClient(fmt.Sprintf(
			"%s:///%s",
			loadbalance.Name,
			rpcAddr,
		),
		opts...
	)

	require.NoError(t, err)
	return api.NewLogClient(conn)
}
