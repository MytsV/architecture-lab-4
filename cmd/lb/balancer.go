package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

// Some as env variables?
var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 10, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type Server struct {
	Url           string
	ConnectionCnt Counter
	IsHealthy     bool
}

type Counter struct {
	mu sync.Mutex
	v  int
}

func (c *Counter) Change(amount int) {
	c.mu.Lock()
	c.v += amount
	c.mu.Unlock()
}

func (c *Counter) Get() int {
	c.mu.Lock()
	res := c.v
	c.mu.Unlock()
	return res
}

func (s *Server) Forward(rw http.ResponseWriter, r *http.Request) error {
	s.ConnectionCnt.Change(1)
	defer func() {
		s.ConnectionCnt.Change(-1)
	}()
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = s.Url
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = s.Url

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", s.Url)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", s.Url, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func (s *Server) CheckHealth() {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), s.Url), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		s.IsHealthy = false
	}
	s.IsHealthy = true
}

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []Server{
		{Url: "server1:8080", IsHealthy: true},
		{Url: "server2:8080", IsHealthy: false},
		{Url: "server3:8080", IsHealthy: true},
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

// get to know about independent tests
// tests for health and test + health communication too
func Balance(pool []Server) int {
	min := -1
	for i := 0; i < len(pool); i++ {
		if !pool[i].IsHealthy {
			continue
		}
		if min < 0 || pool[i].ConnectionCnt.Get() < pool[min].ConnectionCnt.Get() {
			min = i
		}
	}
	return min
}

func main() {
	flag.Parse()

	for i, _ := range serversPool {
		go func(s *Server) {
			for range time.Tick(10 * time.Second) {
				s.CheckHealth()
			}
		}(&serversPool[i])
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// Error if no healthy
		i := Balance(serversPool)
		serversPool[i].Forward(rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
