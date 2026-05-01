// Remote tunnel server — Go port of remote/server.py.
// Listens on a Unix socket, accepts frames from the bridge (client.py),
// opens TCP connections to targets, and relays data back.
// Protocol: newline-delimited  TYPE:CONN_ID:PAYLOAD_B64
package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	sockPath = "/tmp/.wst.sock"
	maxChunk = 2048
)

type frame struct {
	ftype   string
	connID  int
	payload []byte
}

func encodeFrame(ftype string, connID int, payload []byte) []byte {
	return []byte(fmt.Sprintf("%s:%d:%s\n", ftype, connID,
		base64.StdEncoding.EncodeToString(payload)))
}

func decodeFrame(line string) (frame, bool) {
	parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
	if len(parts) < 3 {
		return frame{}, false
	}
	cid, err := strconv.Atoi(parts[1])
	if err != nil {
		return frame{}, false
	}
	var payload []byte
	if parts[2] != "" {
		payload, err = base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			return frame{}, false
		}
	}
	return frame{ftype: parts[0], connID: cid, payload: payload}, true
}

type server struct {
	bridge net.Conn
	out    chan []byte

	mu    sync.Mutex
	conns map[int]net.Conn
}

func newServer(bridge net.Conn) *server {
	return &server{
		bridge: bridge,
		out:    make(chan []byte, 64),
		conns:  make(map[int]net.Conn),
	}
}

func (s *server) send(ftype string, cid int, payload []byte) {
	s.out <- encodeFrame(ftype, cid, payload)
}

func (s *server) writer() {
	for buf := range s.out {
		if _, err := s.bridge.Write(buf); err != nil {
			return
		}
	}
}

func (s *server) handleNew(cid int, target string) {
	idx := strings.LastIndex(target, ":")
	if idx < 0 {
		s.send("A", cid, []byte("ERR:bad_target"))
		return
	}
	host, portStr := target[:idx], target[idx+1:]
	if _, err := strconv.Atoi(portStr); err != nil {
		s.send("A", cid, []byte("ERR:bad_port"))
		return
	}

	c, err := net.DialTimeout("tcp", net.JoinHostPort(host, portStr), 10*time.Second)
	if err != nil {
		msg := fmt.Sprintf("ERR:%.100s", err.Error())
		s.send("A", cid, []byte(msg))
		return
	}

	s.mu.Lock()
	s.conns[cid] = c
	s.mu.Unlock()
	s.send("A", cid, []byte("OK"))

	go s.tcpReader(cid, c)
}

func (s *server) tcpReader(cid int, c net.Conn) {
	buf := make([]byte, maxChunk)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			s.send("D", cid, data)
		}
		if err != nil {
			s.send("C", cid, nil)
			s.closeConn(cid)
			return
		}
	}
}

func (s *server) handleData(cid int, payload []byte) {
	s.mu.Lock()
	c, ok := s.conns[cid]
	s.mu.Unlock()
	if !ok {
		return
	}
	if _, err := c.Write(payload); err != nil {
		s.send("C", cid, nil)
		s.closeConn(cid)
	}
}

func (s *server) closeConn(cid int) {
	s.mu.Lock()
	c, ok := s.conns[cid]
	if ok {
		delete(s.conns, cid)
	}
	s.mu.Unlock()
	if ok {
		c.Close()
	}
}

func (s *server) run() {
	go s.writer()

	if _, err := s.bridge.Write([]byte("TUNNEL_READY\n")); err != nil {
		return
	}

	scanner := bufio.NewScanner(s.bridge)
	scanner.Buffer(make([]byte, 65536), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		f, ok := decodeFrame(line)
		if !ok {
			continue
		}
		switch f.ftype {
		case "N":
			go s.handleNew(f.connID, string(f.payload))
		case "D":
			s.handleData(f.connID, f.payload)
		case "C":
			s.closeConn(f.connID)
		case "P":
			s.send("P", 0, nil)
		}
	}

	s.mu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.conns = nil
	s.mu.Unlock()
	close(s.out)
}

func cleanup() {
	os.Remove(sockPath)
}

func main() {
	cleanup()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cleanup()
		os.Exit(0)
	}()

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer ln.Close()
	defer cleanup()

	bridge, err := ln.Accept()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	newServer(bridge).run()
}
