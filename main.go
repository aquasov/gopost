package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	numberOfWorkers = 1
	bufferSize      = 5
	bufferTimeout   = 10000
)

var queue = make(chan []byte, 1024)
var workerWG = sync.WaitGroup{}
var handlerWG = sync.WaitGroup{}

func handler(w http.ResponseWriter, req *http.Request) {
	handlerWG.Add(1)
	defer handlerWG.Done()
	log.Println("[server] validating request")
	if ok := validate(req); ok {
		if body, err := ioutil.ReadAll(req.Body); err != nil {
			log.Printf("[server] failed to read body: %v", err)
		} else {
			log.Println("[server] request is valid")
			w.WriteHeader(http.StatusOK)
			records := bytes.Split(bytes.TrimSpace(body), []byte("\n"))
			for _, rec := range records {
				log.Printf("[server] posting record to the queue: %v\n", string(rec))
				queue <- rec
			}
		}
	} else {
		log.Println("[server] request is invalid")
		w.WriteHeader(http.StatusBadRequest)
	}
}

func validate(req *http.Request) bool {
	// TODO: request auth
	if req.Method != "POST" {
		return false
	}
	return true
}

func worker(id int, queue <-chan []byte) {
	var buffer = &[bufferSize][]byte{}
	log.Printf("[worker %v] initialized with buffer size %v\n", id, bufferSize)
	var bufferIndex, recordCount int

	callFlush := func() {
		workerWG.Add(1)
		go flush(buffer)
		buffer = &[bufferSize][]byte{}
		bufferIndex = 0
	}

	for open := true; open; {
		timer := time.NewTimer(bufferTimeout * time.Millisecond)
		select {
		case rec, ok := <-queue:
			if !ok {
				open = false
				break
			}
			recordCount++
			log.Printf("[worker %v] buffering record: %v\n", id, string(rec))
			(*buffer)[bufferIndex] = rec
			bufferIndex++
			if bufferIndex == bufferSize {
				log.Printf("[worker %v] flushing buffer by size\n", id)
				callFlush()
			}
		case <-timer.C:
			if len((*buffer)[0]) > 0 {
				log.Printf("[worker %v] flushing buffer by timeout\n", id)
				callFlush()
			}
		}
	}
	if len((*buffer)[0]) > 0 {
		log.Printf("[worker %v] flushing buffer for the last time\n", id)
		callFlush()
	}
	log.Printf("[worker %v] finished, %v records processed\n", id, recordCount)
	workerWG.Done()
}

func flush(buf *[bufferSize][]byte) {
	var jsonMap map[string]interface{}
	for _, rec := range *buf {
		if len(rec) < 1 {
			break
		}
		if err := json.Unmarshal(rec, &jsonMap); err != nil {
			log.Printf("Error paring JSON string '%v': %v", string(rec), err)
			continue
		}
		log.Println(jsonMap)
	}
	workerWG.Done()
}

func main() {

	// start workers

	for i := 0; i < numberOfWorkers; i++ {
		workerWG.Add(1)
		go worker(i, queue)
	}

	// start web server

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/", handler)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Fatal(srv.ListenAndServe())
	}()

	log.Println("[server] started")

	// wait for shutdown event

	shutdown := make(chan os.Signal)
	signal.Notify(
		shutdown,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-shutdown

	// perform shutdown

	fmt.Println("-------- SHUTTING DOWN --------")

	handlerWG.Wait()
	close(queue)
	workerWG.Wait()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("[server] shutdown error: %v\n", err)
	}
}
