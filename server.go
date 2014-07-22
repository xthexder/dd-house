package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

var port = flag.Int("port", 8080, "which port to listen on")
var authApiKey string

func handleIntake(w http.ResponseWriter, req *http.Request) {
	if req.UserAgent() == "Datadog-Status-Check" {
		io.WriteString(w, "Hello\n")
		return
	}
	err := req.ParseForm()
	if err != nil {
		log.Println("Error parsing form:", err)
		http.Error(w, err.Error(), 500)
		return
	}
	values := req.Form
	api_key := values.Get("api_key")
	delete(values, "api_key")
	if len(authApiKey) > 0 && api_key != authApiKey {
		log.Println("Got bad API key:", api_key)
		http.Error(w, "Bad API Key", 403)
		return
	}

	data, err := json.Marshal(values)
	if err != nil {
		log.Println("Failed to marshal json:", err)
		http.Error(w, err.Error(), 500)
		return
	}
	log.Println(req.Method, req.Host, req.URL.Path, api_key, string(data))
	io.WriteString(w, "hello, world!\n")
}

func main() {
	flag.Parse()
	authApiKey = os.Getenv("API_KEY")
	if len(authApiKey) == 0 {
		log.Println("Warning: API_KEY is blank")
	}

	log.Println("dd-house listening on ", *port)

	http.HandleFunc("/intake", handleIntake)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}
