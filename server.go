package main

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
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

func handleApi(w http.ResponseWriter, req *http.Request) {
	if handleApiKey(w, req) {
		return
	}

	dump, _ := httputil.DumpRequest(req, false)
	log.Println(string(dump))

	body := req.Body
	if req.Header.Get("Content-Encoding") == "deflate" {
		var err error
		body, err = zlib.NewReader(body)
		if err != nil {
			log.Println(err)
			io.WriteString(w, `{"status":"failed"}`)
			return
		}
	}
	buf := bytes.NewBuffer([]byte{})
	_, err := io.Copy(buf, body)
	if err != nil {
		log.Println(err)
		io.WriteString(w, `{"status":"failed"}`)
		return
	} else {
		log.Println(buf.String())
	}

	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"status":"ok"}`)
}

func handleIntake(w http.ResponseWriter, req *http.Request) {
	if handleApiKey(w, req) {
		return
	}

	dump, _ := httputil.DumpRequest(req, false)
	log.Println(string(dump))

	body := req.Body
	if req.Header.Get("Content-Encoding") == "deflate" {
		var err error
		body, err = zlib.NewReader(body)
		if err != nil {
			log.Println(err)
			io.WriteString(w, `{"status":"failed"}`)
			return
		}
	}
	buf := bytes.NewBuffer([]byte{})
	_, err := io.Copy(buf, body)
	if err != nil {
		log.Println(err)
		io.WriteString(w, `{"status":"failed"}`)
		return
	}

	data := make(map[string]interface{})
	err = json.Unmarshal(buf.Bytes(), &data)
	if err != nil {
		log.Println(err)
		io.WriteString(w, `{"status":"failed"}`)
		return
	}

	for key, _ := range data {
		fmt.Println(key)
	}

	host := data["internalHostname"].(string)
	go pushStat(host, "system.load.1", data["system.load.1"].(float64))
	go pushStat(host, "system.load.5", data["system.load.5"].(float64))
	go pushStat(host, "system.load.15", data["system.load.15"].(float64))

	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"status":"ok"}`)
}

func pushStat(host, key string, value float64) {
	log.Printf("[%s] %s: %f", host, key, value)

	body := bytes.NewBufferString(`[{"name" : "` + key + `","columns" : ["value", "host"],"points" : [[` + strconv.FormatFloat(value, 'f', 4, 64) + `, "` + host + `"]]}]`)
	resp, err := http.Post("http://localhost:8086/db/datadog/series?u=root&p=root", "application/json", body)
	if err != nil {
		log.Println(err)
	} else {
		log.Println(resp)
	}
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
