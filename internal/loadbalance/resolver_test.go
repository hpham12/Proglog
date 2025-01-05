package loadbalance_test

import (
	api "Proglog/api/v1"
	"Proglog/internal/config"
	"Proglog/internal/loadbalance"
	"Proglog/internal/server"
	"net"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestResolver(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:		config.ServerCertFile,
		KeyFile:		config.ServerKeyFile,
		CAFile:			config.CAFile,
		Server:			true,
		ServerAddress:	"127.0.0.1",
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(tlsConfig)

	srv, err := server.NewGRPCServer(&server.Config{
		GetServerer: &getServers{},
	}, grpc.Creds(serverCreds))

	require.NoError(t, err)

	go srv.Serve(listener)

	con := &clientConn{}

	var _ resolver.ClientConn = (*clientConn)(nil)

	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:		config.RootClientCertFile,
		KeyFile:		config.RootClientKeyFile,
		CAFile:			config.CAFile,
		Server:			false,
		ServerAddress:	"127.0.0.1",
	})

	require.NoError(t, err)

	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}
	r := &loadbalance.Resolver{}
	
	_, err = r.Build(
		resolver.Target{
			URL: url.URL{
				Path: listener.Addr().String(),
			},
		},
		con,
		opts,
	)

	require.NoError(t, err)

	wantState := resolver.State {
		Addresses: []resolver.Address{
			{
				Addr: "localhost:9001",
				Attributes: attributes.New("is_leader", true),
			},
			{
				Addr: "localhost:9002",
				Attributes: attributes.New("is_leader", false),
			},
		},
	}

	require.Equal(t, wantState, con.state)

	con.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, con.state)
}

type getServers struct{}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:			"server1",
			RpcAddr: 	"localhost:9001",
			IsLeader: 	true,
		},
		{
			Id:			"server2",
			RpcAddr: 	"localhost:9002",
			IsLeader: 	false,
		},
	},
	nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(err error) {}

func (c *clientConn) NewServiceConfig(config string) {}

func (c *clientConn) ParseServiceConfig(config string,) *serviceconfig.ParseResult {
	return nil
}
