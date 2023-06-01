package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"strings"

	"github.com/roman-mazur/design-practice-2-template/datastore"
	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var port = flag.Int("port", 8100, "server port")
var db *datastore.Db

func main() {
	flag.Parse()
	h := new(http.ServeMux)
	newDb, err := datastore.NewDb("./out")
	if err != nil {
		panic(err)
	}
	db = newDb

	//db.Put("codebryksy", time.Now().Format("2006-01-02"))

	h.HandleFunc("/db/", handleDb)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

func handleDb(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleDbGet(rw, r)
	case http.MethodPost:
		handleDbPost(rw, r)
	default:
		http.Error(rw, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

type entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func handleDbGet(rw http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	value, err := db.Get(key)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
	} else {
		e := entry{Key: key, Value: value}
		_ = json.NewEncoder(rw).Encode(e)
	}
}

func handleDbPost(rw http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	value := r.FormValue("value")
	err := db.Put(key, value)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
	}
}
