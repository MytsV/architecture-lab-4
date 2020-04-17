package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/roman-mazur/design-practice-3-template/httptools"
	"github.com/roman-mazur/design-practice-3-template/signal"
)

var port = flag.Int("port", 8090, "load balancer port")
var timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
var https = flag.Bool("https", true, "whether backends support HTTPs")

var timeout = time.Duration(*timeoutSec) * time.Second

var serversPool = []string{
	"0aca0fe0cc022516765fe2ce333adb41.balena-devices.com",
	"a7cf5dab491f1583c1962834611eeffc.balena-devices.com",
	"d55cdee2e7aae94f46b438ee405983aa.balena-devices.com",
}

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		// TODO: set team and counter headers.
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	for _, server := range serversPool {
		server := server
		go func() {
			for range time.Tick(10 * time.Second) {
				log.Println(server, health(server))
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		forward(serversPool[0], rw, r)
	}))

	frontend.Start()
	signal.WaitForTerminationSignal()
}
