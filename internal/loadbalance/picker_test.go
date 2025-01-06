package loadbalance_test

import (
	"testing"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/balancer/base"
	"Proglog/internal/loadbalance"

	"github.com/stretchr/testify/require"
)

func TestPickerNoSubConnAvailable(t *testing.T) {
	picker := &loadbalance.Picker{}
	for _, method := range []string{
		"/Proglog/Produce",
		"/Proglog/Consume",
	} {
		info := balancer.PickInfo {
			FullMethodName: method,
		}

		result, err := picker.Pick(info)
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
		require.Nil(t, result.SubConn)
	}
}

func TestPickerForProduceRequest(t *testing.T) {
	subConns, picker := setupTest()
	for _, method := range []string{
		"/Proglog/Produce",
		"/Proglog/ProduceStream",
	} {
		info := balancer.PickInfo {
			FullMethodName: method,
		}

		result, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[0], result.SubConn)
	}
}

func TestPickerForConsumeRequest(t *testing.T) {
	subConns, picker := setupTest()

	info := balancer.PickInfo {
		FullMethodName: "/Proglog/Consume",
	}

	for i := 0; i < 5; i++ {
		result, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[i%2], result.SubConn)
	}
}

func setupTest() ([]*subConn, *loadbalance.Picker) {
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}
	var subConns []*subConn

	for i := 0; i < 3; i++ {
		sc := &subConn{}
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
		}
		buildInfo.ReadySCs[sc] = base.SubConnInfo{
			Address: addr,
		}
		subConns = append(subConns, sc)
	}

	picker := &loadbalance.Picker{}
	picker.Build(buildInfo)

	return subConns, picker
}

var _ balancer.SubConn = (*subConn)(nil)

// subConn implements balancer.SubConn
type subConn struct {
	balancer.SubConn
}
