package embeddedserver

// ZeroStorCluster defines a 0-stor server cluster
// which only intended to be used in test
type ZeroStorCluster struct {
	servers []*zeroStorServer
}

// NewZeroStorCluster creates new 0-stor cluster
func NewZeroStorCluster(num int) (*ZeroStorCluster, error) {
	var servers []*zeroStorServer

	for i := 0; i < num; i++ {
		server, err := newZeroStorServer()
		if err != nil {
			return nil, err
		}
		servers = append(servers, server)
	}

	return &ZeroStorCluster{
		servers: servers,
	}, nil
}

// Addrs returns slice of each server address
func (z *ZeroStorCluster) Addrs() []string {
	var addrs []string
	for _, s := range z.servers {
		addrs = append(addrs, s.Addr())
	}
	return addrs
}

// Close stop and release all server's resources
func (z *ZeroStorCluster) Close() {
	for _, s := range z.servers {
		s.Close()
	}
}
