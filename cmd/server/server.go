package main

import (
	"context"
	"flag"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var port = flag.Int("port", 8080, "server port")
var delay = flag.Int("delay", 0, "response delay in millseconds")
var healthInit = flag.Bool("health", true, "initial server health")
var debug = flag.Bool("debug", false, "whether we can change server's health status")
var dbUrl = flag.String("db-url", "db:8100", "hostname of database service")

const scheme = "http"
const team = "codebryksy"

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

var report Report

func main() {
	flag.Parse()
	h := new(http.ServeMux)
	health := boolMutex{v: *healthInit}
	writeTeam()

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

	report = make(Report)

	h.HandleFunc("/api/v1/some-data", handleDefaultGet)

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

func writeTeam() {
	path := scheme + "://" + *dbUrl + "/db/" + team
	formData := url.Values{}
	formData.Set("value", time.Now().Format("2006-01-02"))

	resp, err := http.PostForm(path, formData)
	if err != nil || resp.StatusCode != http.StatusOK {
		panic("Can't initiate DB")
	}
}

func handleDefaultGet(rw http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(10)*time.Second)
	defer cancel()
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = *dbUrl
	fwdRequest.Host = *dbUrl
	fwdRequest.URL.Scheme = scheme
	fwdRequest.URL.Path = "/db/" + key

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	if *delay > 0 && *delay < 300 {
		time.Sleep(time.Duration(*delay) * time.Millisecond)
	}

	report.Process(r)

	rw.WriteHeader(resp.StatusCode)
	rw.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	rw.Header().Set("Content-Length", resp.Header.Get("Content-Length"))
	io.Copy(rw, resp.Body)
	resp.Body.Close()
}
