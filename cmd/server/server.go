package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"time"
	"sync"
	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var port = flag.Int("port", 8080, "server port")
var delay = flag.Int("delay-sec", 0, "response delay in seconds")
var healthInit = flag.Bool("health", true, "initial server health")
var debug = flag.Bool("debug", false, "whether we can change server's health status")

type boolMutex struct {
	mu sync.Mutex
	v  bool
}

func (c *boolMutex) Inverse() {
	c.mu.Lock()
	c.v = !c.v
	c.mu.Unlock()
}

func (c *boolMutex) Get() bool {
	c.mu.Lock()
	res := c.v
	c.mu.Unlock()
	return res
}


func main() {
	flag.Parse()
	h := new(http.ServeMux)
	health := boolMutex{v: *healthInit}

	if *debug {
		h.HandleFunc("/inverse-health", func(rw http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				http.Error(rw, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
			health.Inverse()
		})
	}

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if health.Get() {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		} else {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		if *delay > 0 && *delay < 300 {
			time.Sleep(time.Duration(*delay) * time.Second)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode([]string{
			"1", "2",
		})
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
