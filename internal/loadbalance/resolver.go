package resolver

import (
	"context"
	"fmt"
	"sync"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	api "Proglog/api/v1"
)

// Resolver is the type we will implement into gRPC's resolver.Builder 
// and resolver.Resolver interfaces
type Resolver struct {
	mu				sync.Mutex
	// clientConn is the user's client connection and gRPC passes it to the resolver for the resolver to update with the servers it discovers
	clientConn		resolver.ClientConn	
	// resolverConn is the resolver's own client connection to the server so it can call
	// GetServers() and get the servers
	resolverConn	*grpc.ClientConn
	serviceConfig 	*serviceconfig.ParseResult
	logger 			*zap.Logger
}

var _ resolver.Builder = (*Resolver)(nil)

const Name = "proglog"

// This method receives the data needed to build a resolver that can discover
// the servers (like the target address) and the client connection that the
// resolver will update with the servers it discovers. It also sets up a client
// connection to our server so the resolver can call the GetServers() API
func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)

	var err error
	r.resolverConn, err = grpc.NewClient(target.Endpoint(), dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

// Returns the resolver's scheme identifier
func (r *Resolver) Scheme() string {
	return Name
}

// register this resolver with gRPC so gRPC knows about this resolver
// when it looks for resolvers that match the target's scheme
func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Resolver = (*Resolver)(nil)

// gRPC calls ResolveNow() to resolve the target, discover the servers, and
// update the client connection with the servers
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}

	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader,
			),
		})
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses: addrs,
		ServiceConfig: r.serviceConfig,
	})
}

// Closes the resolver by closing the connection to our server created in Build()
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
