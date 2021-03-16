package main

import (
	"github.com/kalfonso/proglog/apps/inmemorylog/internal/server/http"
	"log"
)

func main() {
	srv := http.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
