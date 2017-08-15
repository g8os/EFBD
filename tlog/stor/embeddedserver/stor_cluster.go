package embeddedserver

type ZeroStorCluster struct {
	servers []*ZeroStorServer
}

func NewZeroStorCluster(num int) (*ZeroStorCluster, error) {
	var servers []*ZeroStorServer

	for i := 0; i < num; i++ {
		server, err := NewZeroStorServer()
		if err != nil {
			return nil, err
		}
		servers = append(servers, server)
	}

	return &ZeroStorCluster{
		servers: servers,
	}, nil
}

func (z *ZeroStorCluster) Addrs() []string {
	var addrs []string
	for _, s := range z.servers {
		addrs = append(addrs, s.Addr())
	}
	return addrs
}

func (z *ZeroStorCluster) Close() {
	for _, s := range z.servers {
		s.Close()
	}
}
