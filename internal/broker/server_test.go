package broker

import (
	"net"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	lc := net.ListenConfig{}
	broker := NewBroker("localhost", "7651", lc)

	go broker.Start()

	time.Sleep(1 * time.Second)

	if broker.Addr == "" {
		t.Error("Fail to start server")
	}
	time.Sleep(5 * time.Second)

}
