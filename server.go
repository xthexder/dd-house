package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

var port = flag.Int("port", 8080, "which port to listen on")
var authApiKey string

func handleApiKey(w http.ResponseWriter, req *http.Request) bool {
	if req.UserAgent() == "Datadog-Status-Check" {
		io.WriteString(w, "Hello\n")
		return true
	}
	err := req.ParseForm()
	if err != nil {
		log.Println("Error parsing form:", err)
		http.Error(w, err.Error(), 500)
		return true
	}
	values := req.Form
	api_key := values.Get("api_key")
	delete(values, "api_key")
	if len(authApiKey) > 0 && api_key != authApiKey {
		log.Println("Got bad API key:", api_key)
		http.Error(w, "Bad API Key", 403)
		return true
	}
	return false
}

func handleIntake(w http.ResponseWriter, req *http.Request) {
	if handleApiKey(w, req) {
		return
	}

	log.Println(req.Method, req.URL.String())
	buf := make([]byte, 64*1024)
	io.ReadFull(req.Body, buf)
	log.Println(string(buf))
	io.WriteString(w, "AgentHandler is running")
}

func handleApi(w http.ResponseWriter, req *http.Request) {
	if handleApiKey(w, req) {
		return
	}

	log.Println(req.Method, req.URL.String())
	buf := make([]byte, 64*1024)
	io.ReadFull(req.Body, buf)
	log.Println(string(buf))
	io.WriteString(w, "hello, world!\n")

}

func main() {
	flag.Parse()
	authApiKey = os.Getenv("API_KEY")
	if len(authApiKey) == 0 {
		log.Println("Warning: API_KEY is blank")
	}

	log.Println("dd-house listening on", *port)

	http.HandleFunc("/intake", handleIntake)
	http.HandleFunc("/api/v1/series/", handleApi)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}
