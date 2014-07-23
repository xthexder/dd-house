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
	"os"
	"strconv"
	"strings"
)

var port = flag.Int("port", 8080, "which port to listen on")
var authApiKey string
var dbUrl string

var rootMetrics = map[string]string{
	"agentVersion": "host.agent_version",
	"os":           "host.os",
	"python":       "host.python_version",
	"uuid":         "host.uuid",

	"system.load.1":       "system.load.1",
	"system.load.5":       "system.load.5",
	"system.load.15":      "system.load.15",
	"system.load.norm.1":  "system.load.norm.1",
	"system.load.norm.5":  "system.load.norm.5",
	"system.load.norm.15": "system.load.norm.15",

	"cpuIdle":   "system.cpu.idle",
	"cpuUser":   "system.cpu.user",
	"cpuWait":   "system.cpu.iowait",
	"cpuSystem": "system.cpu.system",
	"cpuStolen": "system.cpu.stolen",

	"memBuffers":       "system.mem.buffered",
	"memPhysPctUsable": "system.mem.pct_usable",
	"memShared":        "system.mem.shared",
	"memPhysTotal":     "system.mem.total",
	"memPhysUsable":    "system.mem.usable",
	"memCached":        "system.mem.cached",
	"memPhysUsed":      "system.mem.used",
	"memPhysFree":      "system.mem.free",

	"memSwapFree":    "system.swap.free",
	"memSwapTotal":   "system.swap.total",
	"memSwapUsed":    "system.swap.used",
	"memSwapPctFree": "system.swap.pct_free",
}

var diskMetrics = []string{
	"device",
	"total",
	"used",
	"free",
	"in_use",
	"mount",
}

type Metric struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

func NewMetric(host, name string, timestamp uint64, value interface{}, tags map[string]string) *Metric {
	columns := []string{"time", "value", "hostname"}
	points := [][]interface{}{{timestamp, value, host}}
	if tags != nil {
		for k, v := range tags {
			if k == "hostname" {
				points[0][2] = v
			} else {
				columns = append(columns, k)
				points[0] = append(points[0], v)
			}
		}
	}
	return &Metric{name, columns, points}
}

func NewMetricGroup(host, name string, timestamp uint64, values map[string]interface{}, tags map[string]string) *Metric {
	columns := []string{"time", "hostname"}
	points := [][]interface{}{{timestamp, host}}
	for k, v := range values {
		columns = append(columns, k)
		points[0] = append(points[0], v)
	}
	if tags != nil {
		for k, v := range tags {
			if k == "hostname" {
				points[0][1] = v
			} else {
				columns = append(columns, k)
				points[0] = append(points[0], v)
			}
		}
	}
	return &Metric{name, columns, points}
}

func PushMetrics(metrics []*Metric) {
	if len(metrics) == 0 {
		return
	}
	log.Printf("Pushing %d metrics to InfluxDB\n", len(metrics))

	body, err := json.Marshal(metrics)
	if err != nil {
		log.Println(err)
		return
	}

	resp, err := http.Post(dbUrl, "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Println(err)
	} else if resp.StatusCode != 200 {
		log.Println("Got Response: ", resp.Status)
	}
}

func mapMetrics(data map[string]interface{}) []*Metric {
	metrics := []*Metric{}
	host := data["internalHostname"].(string)
	timestamp := uint64(data["collection_timestamp"].(float64) * 1000)

	delete(data, "apiKey")
	delete(data, "internalHostname")
	delete(data, "collection_timestamp")

	values := make(map[string]map[string]interface{})

	log.Printf("Parsing metrics for: %s\n", host)
	for key, value := range data {
		name, ok := rootMetrics[key]
		if ok {
			index := strings.LastIndexAny(name, ".")
			group_name := name[:index]
			group := values[group_name]
			if group == nil {
				group = make(map[string]interface{})
				values[group_name] = group
			}
			group[name[index+1:]] = value
			delete(data, key)
		}
	}
	for name, group := range values {
		metrics = append(metrics, NewMetricGroup(host, name, timestamp, group, nil))
	}

	metrics = append(metrics, mapDiskMetrics("system.disk", timestamp, host, data["diskUsage"].([]interface{})))
	delete(data, "diskUsage")
	metrics = append(metrics, mapDiskMetrics("system.fs.inodes", timestamp, host, data["inodes"].([]interface{})))
	delete(data, "inodes")

	debug, err := json.MarshalIndent(data, "", "  ")
	if err == nil {
		fmt.Println(string(debug))
	}
	return metrics
}

func mapDiskMetrics(name string, timestamp uint64, host string, data []interface{}) *Metric {
	columns := append([]string{"time", "hostname"}, diskMetrics...)
	points := make([][]interface{}, len(data))
	metric := &Metric{name, columns, points}
	for i, disk := range data {
		fields := disk.([]interface{})
		percent := fields[4].(string)
		parse, _ := strconv.ParseFloat(percent[:len(percent)-1], 64)
		fields[4] = parse / 100.0
		points[i] = append([]interface{}{timestamp, host}, fields...)
	}
	return metric
}

func handleIntake(w http.ResponseWriter, req *http.Request) {
	if handleApiKey(w, req) {
		return
	}

	log.Println(req.Method, req.URL.Path)

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

	metrics := mapMetrics(data)
	go PushMetrics(metrics)

	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"status":"ok"}`)
}

func handleApi(w http.ResponseWriter, req *http.Request) {
	if handleApiKey(w, req) {
		return
	}

	log.Println(req.Method, req.URL.Path)

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

func handleApiKey(w http.ResponseWriter, req *http.Request) bool {
	if req.UserAgent() == "Datadog-Status-Check" {
		io.WriteString(w, "STILL-ALIVE\n")
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

func main() {
	flag.Parse()
	authApiKey = os.Getenv("API_KEY")
	if len(authApiKey) == 0 {
		log.Println("Warning: API_KEY is blank")
	}
	dbUrl = os.Getenv("DB_URL")
	if len(dbUrl) == 0 {
		dbUrl = "http://localhost:8086/db/datadog/series?u=root&p=root"
	}

	log.Println("dd-house listening on", *port)

	http.HandleFunc("/intake", handleIntake)
	http.HandleFunc("/api/v1/series/", handleApi)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*port), nil))
}
