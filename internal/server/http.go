package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// ProduceRequest represents a request to append a value to the commit log
type ProduceRequest struct {
	Value []byte `json:"value"`
}

// ProduceResponse represents a response from appending a value to the commit log
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest represents a request to consume a record from the log at an offset
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse represents a response to consume record request. It returns the value and offset.
type ConsumeResponse struct {
	Record Record `json:"record"`
}

// NewHttpServer creates a new HTTP server to expose the commit log API
func NewHttpServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, errors.WithStack(err).Error(), http.StatusBadRequest)
		return
	}
	offset, err := s.Log.Append(req.Value)
	if err != nil {
		http.Error(w, errors.WithStack(err).Error(), http.StatusInternalServerError)
		return
	}
	resp := ProduceResponse{
		Offset: offset,
	}
	err = json.NewEncoder(w).Encode(&resp)
	if err != nil {
		http.Error(w, errors.WithStack(err).Error(), http.StatusInternalServerError)
	}
	return
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, errors.WithStack(err).Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err != nil {
		http.Error(w, errors.WithStack(err).Error(), http.StatusInternalServerError)
		return
	}
	resp := &ConsumeResponse{
		Record: record,
	}
	err = json.NewEncoder(w).Encode(&resp)
	if err != nil {
		http.Error(w, errors.WithStack(err).Error(), http.StatusInternalServerError)
	}
	return
}
