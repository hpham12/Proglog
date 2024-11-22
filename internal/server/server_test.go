package server

import (
	api "Proglog/api/v1"
	"Proglog/internal/config"
	"Proglog/internal/log"
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		conig *Config,
	) {
		"produce/consme a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds": testProduceConsumeStream,
		"consume past log boundary fails": testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func createNewClient(clientCertFile string, clientKeyFile string, caFile string, l net.Listener) (*grpc.ClientConn, api.LogClient, error) {
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile: caFile,
		CertFile: clientCertFile,
		KeyFile: clientKeyFile,
		Server: false,
	})
	if err != nil {
		return nil, nil, err
	}

	clientCreds := credentials.NewTLS(clientTLSConfig)
	cc, err := grpc.NewClient(
		l.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
	)
	if err != nil {
		return nil, nil, err
	}

	return cc, api.NewLogClient(cc), nil
}

func setupTest(t *testing.T, fn func(*Config))(nobodyClient api.LogClient, rootClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// create a listener on the local network address that our server will run on,
	// using a random free port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	rootClientConn, rootClient, err := createNewClient(config.RootClientCertFile, config.RootClientKeyFile, config.CAFile, l)
	require.NoError(t, err)

	nobodyClientConn, nobodyClient, err := createNewClient(config.NobodyClientCertFile, config.NobodyClientKeyFile, config.CAFile, l)
	require.NoError(t, err)
	
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile: config.ServerKeyFile,
		CAFile: config.CAFile,
		ServerAddress: l.Addr().String(),
		Server: true,
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return nobodyClient, rootClient, cfg, func() {
		server.Stop()
		rootClientConn.Close()
		nobodyClientConn.Close()
		l.Close()
	}
}

func testProduceConsume(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := rootClient.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := rootClient.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := rootClient.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	
	require.NoError(t, err)
	consume, err := rootClient.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset + 1,
		},
	)
	require.Nil(t, consume)
	require.Error(t, err)
}

func testProduceConsumeStream(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient, config *Config) {
	ctx := context.Background()
	wants := [](*api.Record) {
		&api.Record{Value: []byte("hello world 1")},
		&api.Record{Value: []byte("hello world 2")},
		&api.Record{Value: []byte("hello world 3")},
	}

	bidiStreamingClient, err := rootClient.ProduceStream(ctx)
	require.NoError(t, err)

	for index, want := range wants {
		bidiStreamingClient.Send(&api.ProduceRequest{
			Record: want,
		})
		res, _ := bidiStreamingClient.Recv()
		require.Equal(t, uint64(index), res.Offset)
	}

	// exit the ProduceStream() method in server early because .Recv() will
	// thorw EOF error
	bidiStreamingClient.CloseSend()

	serverStreamingClient, err := rootClient.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)

	require.NoError(t, err)
	// after the last .Recv(), a signal is sent to stream.Context().Done() channel
	for _, want := range wants {
		response, err := serverStreamingClient.Recv()
		require.NoError(t, err)
		require.Equal(t, response.Record.Value, want.Value)
	}
}
