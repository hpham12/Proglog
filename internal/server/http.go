package server;

import (
	"encoding/json"
	"net/http"
	"github.com/gorilla/mux"
)

func NewHTTPServer(address string) *http.Server {
	httpServer := newHTTPServer()
	router := mux.NewRouter()
	router.HandleFunc("/", httpServer.handleProduce).Methods("POST")
	router.HandleFunc("/", httpServer.handleConsume).Methods("GET")
	return &http.Server{
		Addr: address,
		Handler: router,
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

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (server *httpServer) handleProduce(writer http.ResponseWriter, request *http.Request) {
	var decodedRequest ProduceRequest
	err := json.NewDecoder(request.Body).Decode(&decodedRequest)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := server.Log.Append(decodedRequest.Record)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	response := ProduceResponse{Offset: offset}
	err = json.NewEncoder(writer).Encode(response)
	if (err != nil) {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *httpServer) handleConsume(writer http.ResponseWriter, request *http.Request) {
	var decodedRequest ConsumeRequest
	err := json.NewDecoder(request.Body).Decode(&decodedRequest)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := server.Log.Read(decodedRequest.Offset)
	if err == ErrOffsetNotFound {
		http.Error(writer, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(writer).Encode(res)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}
