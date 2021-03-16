package http

import (
	"encoding/json"
	"github.com/kalfonso/proglog/apps/inmemorylog/internal/log"
	"net/http"

	"github.com/gorilla/mux"
)

//ProduceRequest the producer request.
type ProduceRequest struct {
	Record log.Record `json:"record"`
}

//ProduceResponse the producer response.
type ProduceResponse struct {
	Offset uint64 `json:"record"`
}

//ConsumeRequest the consumer request.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

//ConsumeResponse the consumer response.
type ConsumeResponse struct {
	Record log.Record `json:"record"`
}

type httpLogServer struct {
	Log log.Log
}

func NewHTTPServer(addr string) *http.Server {
	logServer := newHTTPLogServer()
	r := mux.NewRouter()
	r.HandleFunc("/", logServer.handleProduce).Methods("POST")
	r.HandleFunc("/", logServer.handleConsume).Methods("GET")
	return &http.Server{
		Addr: addr,
		Handler: r,
	}
}

func newHTTPLogServer() *httpLogServer {
	return &httpLogServer{
		Log: log.NewInMemoryLog(),
	}
}

func (l *httpLogServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := l.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (l *httpLogServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := l.Log.Read(req.Offset)
	if err != nil {
		statusCode := http.StatusInternalServerError
		_, isNotFoundErr := err.(*log.ErrorOffsetNotFound)
		if isNotFoundErr {
			statusCode = http.StatusNotFound
		}
		http.Error(w, err.Error(), statusCode)
		return
	}
	resp := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
