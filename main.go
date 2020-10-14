package main

import (
	"bytes"
	"context"
	"database/sql"
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

	_ "github.com/ClickHouse/clickhouse-go"
)

const (
	numberOfBuffers = 2
	bufferSize      = 7
	bufferTimeout   = 5000
)

const insertTemplate = "INSERT INTO events_buffer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

var jsonKeys = []string{
	"client_time",
	"client_time",
	"device_id",
	"device_os",
	"session",
	"sequence",
	"event",
}

type Event struct {
	clientDate      string
	clientTime      string
	deviceID        string
	deviceOS        string
	session         string
	sequence        int
	event           string
	attributeKeys   []string
	attributeValues []string
	metricKeys      []string
	metricValues    []float64
}

var queue = make(chan []byte, 1024)
var bufferWG = sync.WaitGroup{}
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

func buffer(id int, queue <-chan []byte) {
	var buffer = &[]Event{}
	log.Printf("[buffer %v] initialized with buffer size %v\n", id, bufferSize)
	var bufferIndex, recordCount int

	db, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatalf("[buffer %v] failed to open database connection:\n%v\n", id, err)
	}

	goFlushBuffer := func(renew bool) {
		bufferWG.Add(1)
		go flushBuffer(buffer, db)
		if renew {
			buffer = &[]Event{}
			bufferIndex = 0
		}
	}

	for open := true; open; {
		timer := time.NewTimer(bufferTimeout * time.Millisecond)
		select {
		case rec, ok := <-queue:
			if !ok {
				open = false
				break
			}
			if event, err := transformRecordToEvent(&rec); err != nil {
				log.Printf("[buffer %v] failed to transform record:\nrec: %v\nerr: %v\n", id, string(rec), err)
				logRecordError(string(rec), err)
				break
			} else {
				recordCount++
				log.Printf("[buffer %v] buffering record: %v\n", id, event)
				*buffer = append(*buffer, event)
				bufferIndex++
				if bufferIndex == bufferSize {
					log.Printf("[buffer %v] flushing buffer by size\n", id)
					goFlushBuffer(true)
				}
			}
		case <-timer.C:
			if len(*buffer) > 0 {
				log.Printf("[buffer %v] flushing buffer by timeout\n", id)
				goFlushBuffer(true)
			}
		}
	}
	if len(*buffer) > 0 {
		log.Printf("[buffer %v] flushing buffer for the last time\n", id)
		goFlushBuffer(false)
	}
	log.Printf("[buffer %v] finished, %v records processed\n", id, recordCount)
	bufferWG.Done()
}

func transformRecordToEvent(r *[]byte) (Event, error) {

	var m map[string]interface{}
	if err := json.Unmarshal(*r, &m); err != nil {
		return Event{}, err
	}

	event := Event{
		clientDate:      m["client_time"].(string)[0:10],
		clientTime:      m["client_time"].(string),
		deviceID:        m["device_id"].(string),
		deviceOS:        m["device_os"].(string),
		session:         m["session"].(string),
		sequence:        int(m["sequence"].(float64)),
		event:           m["event"].(string),
		attributeKeys:   []string{},
		attributeValues: []string{},
		metricKeys:      []string{},
		metricValues:    []float64{},
	}

	for k, v := range m {
		if arrayOfStringsContainsValue(jsonKeys, k) {
			continue
		}
		switch v.(type) {
		case int, float64:
			event.metricKeys = append(event.metricKeys, k)
			event.metricValues = append(event.metricValues, v.(float64))
		default:
			event.attributeKeys = append(event.attributeKeys, k)
			event.attributeValues = append(event.attributeValues, v.(string))
		}
	}

	return event, nil
}

func arrayOfStringsContainsValue(a []string, v string) bool {
	for _, s := range a {
		if s == v {
			return true
		}
	}
	return false
}

func logRecordError(r string, e error) {
	// TODO: keep error log somewhere
}

func flushBuffer(buf *[]Event, db *sql.DB) {

	tx, _ := db.Begin()
	st, _ := tx.Prepare(insertTemplate)
	defer st.Close()

	for _, event := range *buf {
		if _, err := st.Exec(
			event.clientDate,
			event.clientTime,
			event.deviceID,
			event.deviceOS,
			event.session,
			event.sequence,
			event.event,
			event.attributeKeys,
			event.attributeValues,
			event.metricKeys,
			event.metricValues,
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	bufferWG.Done()
}

func main() {

	// start buffers

	for i := 0; i < numberOfBuffers; i++ {
		bufferWG.Add(1)
		go buffer(i, queue)
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
	bufferWG.Wait()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("[server] shutdown error: %v\n", err)
	}
}
