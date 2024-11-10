package server

import (
	api "Proglog/api/v1"
	"Proglog/internal/log"
	"context"
	"os"
	"net"
	"testing"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		conig *Config,
	) {
		"produce/consme a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds": testProduceConsumeStream,
		"consume past log boundary fails": testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config))(client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// crete a listener on the local network address that our server will run on,
	// using a random free port
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// create a client that is used to hit the server
	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	cc, err := grpc.NewClient(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)
	
	commitLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config {
		CommitLog: commitLog,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()
	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		commitLog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	
	require.NoError(t, err)
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset + 1,
		},
	)
	require.Nil(t, consume)
	require.Error(t, err)
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	wants := [](*api.Record) {
		&api.Record{Value: []byte("hello world 1")},
		&api.Record{Value: []byte("hello world 2")},
		&api.Record{Value: []byte("hello world 3")},
	}

	bidiStreamingClient, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	for index, want := range wants {
		bidiStreamingClient.Send(&api.ProduceRequest{
			Record: want,
		})
		res, _ := bidiStreamingClient.Recv()
		require.Equal(t, uint64(index), res.Offset)
	}
	bidiStreamingClient.CloseSend()

	serverStreamingClient, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)

	require.NoError(t, err)
	for _, want := range wants {
		response, err := serverStreamingClient.Recv()
		require.NoError(t, err)
		require.Equal(t, response.Record.Value, want.Value)
	}
}
