package discovery

import (
	"net"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Membership is out type wrapping Serf to provide discovery and cluster membership
// to our service
type Membership struct {
	Config
	handler Handler
	serf	*serf.Serf
	events 	chan serf.Event
	logger 	*zap.Logger
}

type Config struct {
	NodeName		string
	BindAddr		string
	Tags			map[string]string
	StartJoinAddrs	[]string
}

// handler represents some component in our service that needs to know
// when a server joins or leaves the cluster
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// creates and configures a Serf instance and starts the eventsHandler goroutine
// to handle Serf's events
func (m *Membership) setupSerf() (error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()

	// Serf listens on this address and port for gossiping
	(*config.MemberlistConfig).BindAddr = addr.IP.String()
	(*config.MemberlistConfig).BindPort = addr.Port

	// The event channel is how we will receive Serf's events when a node
	// joins or leaves the cluster
	m.events = make(chan serf.Event)
	config.EventCh = m.events

	// Serf shares these tags with the other ndoes in the cluster and should use
	// these tags for simple data that informs the cluster how to handle this node
	//
	// For example, Consul shares each node's RPC address with Serf tags, and once
	// they know each other's RPC address, they can make RPCs to each other
	config.Tags = m.Tags

	// NodeName acts as the node's unique identifier across the Serf cluster
	config.NodeName = m.Config.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()

	// When we have an exisiting cluster and we create a new node that we want to 
	// add to that cluster, we need to point our new node to at least one of the nodes
	// now in the cluster. After the new node connects to one of those nodes in the
	// cluster, it will learn about the rest of the nodes, and vice versa
	//
	// StartJoinAddrs field is how we configure new nodes to join an existing cluster
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// Function to create a Membership with the required configuration and event handler
func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config: config,
		handler: handler,
		logger: zap.L().Named("membership"),
	}

	if err := c.setupSerf(); err != nil {
		return nil, err
	}

	return c, nil
}

// Runs in a loops reading event sent by Serf into the events channel, handling
// each incoming event according to the event's type. When a node joins or leaves
// the cluster, Serf sends an event to all nodes, including the node that joined
// or left the cluster
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}

		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// reutrns whether the given Serf member is the local member by checking the member's name
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Returns a point-in-time snapshot of the cluster's Serf members
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Tells this member to leave the cluster
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// Logs the given error and message
func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
