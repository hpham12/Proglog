package main

import (
	"Proglog/internal/server"
	"fmt"
	"log"
)

func main() {
	server := server.NewHTTPServer(":8080")
	log.Fatal(server.ListenAndServe())
	fmt.Print("Hello")
}