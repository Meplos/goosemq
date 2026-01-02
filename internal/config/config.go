package config

import "net"

type ConnexionInfo struct {
	Host string
	Port string
}

func (c *ConnexionInfo) Addr() string {
	return net.JoinHostPort(c.Host, c.Port)
}
