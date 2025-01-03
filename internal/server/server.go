package server

import (
	"context"
	"time"
	api "Proglog/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	zap "go.uber.org/zap"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

// This is used so that we can use different implementations of log and authorizer
type Config struct {
	CommitLog CommitLog
	Authorizer Authorizer
	GetServerer GetServerer
}

type GetServerer interface {
	GetServers() ([]*api.Server, error)
}

const (
	objectWildcard 	= "*"
	produceAction	= "produce"
	consumeAction	= "consume"
)

// compile-time check for type
var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGRPCServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer {
		Config: config,
	}

	return srv, nil
}

// handles request made by client to produce the log record
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error,
) {
	// Performe authorization
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// handles request made by client to consume a log record
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error,
) {
	// Performe authorization
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}


// implements a bidirectional streaming RPC so the client can stream data into
// the server's log and the server can tell the client whether each request succeeded
func (s *grpcServer) ProduceStream (stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		produced, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(produced); err != nil {
			return err;
		}
	}
}

// Implements a server-side streaming RPC so the client can tell the server where in the log 
// to read records, and then the server will stream every record that follows—even records 
// that aren’t in the log yet
func (s *grpcServer) ConsumeStream (
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <- stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// fetch all servers
func (s *grpcServer) GetServers(ctx context.Context, req *api.GetServersRequest) (
	*api.GetServersResponse, error,
) {
	servers, err := s.GetServerer.GetServers()
	if err != nil {
		return nil, err
	}

	return &api.GetServersResponse{
		Servers: servers,
	}, nil
}

// This interface allows us to pass in different log implementations
// and make the service easier to write and test against
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (
	*grpc.Server, 
	error,
) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(func (duration time.Duration) zapcore.Field {
			return zap.Int64(
				"grpc.time_ns",
				duration.Nanoseconds(),
			)
		}),
	}
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	opts = append(
		opts, 
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_auth.StreamServerInterceptor(authenticate),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_auth.UnaryServerInterceptor(authenticate),
				grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
			),
		),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)
	gsrv := grpc.NewServer(opts...)
	srv, err := newGRPCServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// This is a request interceptor that read the subject out of the client's
// cert and writes it to the RPC's context.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"could not find peer info",
		).Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

// Extract the client's cert's subject so we can verify the client and do authz
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
