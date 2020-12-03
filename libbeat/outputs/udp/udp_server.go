package udp

import (
	"context"
	"github.com/elastic/beats/libbeat/common"
	"net"
	"strings"
	"sync"
)

type UDPServer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	messages   []string
	address    string
	mutex      sync.Mutex
}

func MakeTestUDPServer(address string) *UDPServer {

	ctx, cancelFunc := context.WithCancel(context.Background())
	return &UDPServer{address: address,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

func (server *UDPServer) close() {
	server.cancelFunc()
}

func (server *UDPServer) getBuffer() string {
	return ""
}

func (server *UDPServer) getHostAndPortConfig() *common.Config {

	splitAddress := strings.Split(server.address, ":")
	config, _ := common.NewConfigFrom(map[string]interface{}{
		"hosts": []string{splitAddress[0]},
		"port":  splitAddress[1],
	})
	return config
}

func (server *UDPServer) getMessages() []string {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	messages := server.messages
	return messages
}

func (server *UDPServer) serve() (err error) {
	pc, err := net.ListenPacket("udp", server.address)
	if err != nil {
		return
	}
	defer pc.Close()
	doneChan := make(chan error, 1)
	go func() {
		for {
			buffer := make([]byte, 1024)
			bytesRead, _, err := pc.ReadFrom(buffer)

			server.mutex.Lock()
			server.messages = append(server.messages, string(buffer[0:bytesRead]))
			server.mutex.Unlock()

			if err != nil {
				doneChan <- err
				return
			}
		}
	}()

	select {
	case <-server.ctx.Done():
		err = server.ctx.Err()
	case err = <-doneChan:
	}

	return err
}
