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

var processMetrics = []string{
	"user",
	"pid",
	"pct_cpu",
	"pct_mem",
	"vsz",
	"rss",
	"tty",
	"stat",
	"started",
	"running_time",
	"command",
}

var diskMetrics = []string{
	"device",
	"total",
	"used",
	"free",
	"in_use",
	"mount",
}

var ioMetrics = []string{
	"util",
	"avg_q_sz",
	"avg_rq_sz",
	"await",
	"r_s",
	"r_await",
	"rkb_s",
	"rrqm_s",
	"svctm",
	"w_s",
	"w_await",
	"wkb_s",
	"wrqm_s",
}
var ioMetricMapping = map[string]string{
	"util":      "%util",
	"avg_q_sz":  "avgqu-sz",
	"avg_rq_sz": "avgrq-sz",
	"await":     "await",
	"r_s":       "r/s",
	"r_await":   "r_await",
	"rkb_s":     "rkB/s",
	"rrqm_s":    "rrqm/s",
	"svctm":     "svctm",
	"w_s":       "w/s",
	"w_await":   "w_await",
	"wkb_s":     "wkB/s",
	"wrqm_s":    "wrqm/s",
}

type Metric struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

type StatsdSeries struct {
	Series []*StatsdMetric `json:"series"`
}

type StatsdMetric struct {
	Tags     []string        `json:"tags"`
	Metric   string          `json:"metric"`
	Interval float64         `json:"interval"`
	Host     string          `json:"host"`
	Points   [][]interface{} `json:"points"`
	Type     string          `json:"type"`
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
			if k == "hostname" && len(v) > 0 {
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
		dump, _ := httputil.DumpResponse(resp, true)
		log.Printf("Got Response: %s\n%s\n\n\n%s", resp.Status, body, string(dump))
	}
}

func GroupMetric(name string) (string, string) {
	split := strings.SplitN(name, ".", 3)
	if len(split) > 2 {
		return split[0] + "." + split[1], split[2]
	} else {
		return split[0], split[1]
	}
}

func mapStatsd(series []*StatsdMetric) []*Metric {
	metrics := make([]*Metric, len(series))
	host := ""
	for i, metric := range series {
		if len(metric.Host) > 0 {
			host = metric.Host
		}
		columns := []string{"time", "value", "hostname"}
		points := metric.Points
		for i, _ := range points {
			points[i][0] = uint64(points[i][0].(float64) * 1000)
			points[i] = append(points[i], host)
		}
		columns = append(columns, "metric_interval", "metric_type")
		for i, _ := range points {
			points[i] = append(points[i], metric.Interval, metric.Type)
		}
		if metric.Tags != nil {
			for _, tag := range metric.Tags {
				split := strings.SplitN(tag, ":", 2)
				if split[0] == "hostname" {
					for i, _ := range points {
						points[i][2] = split[1]
					}
				} else {
					columns = append(columns, split[0])
					for i, _ := range points {
						points[i] = append(points[i], split[1])
					}
				}
			}
		}
		metrics[i] = &Metric{"statsd." + metric.Metric, columns, points}
	}
	for _, metric := range metrics {
		for _, points := range metric.Points {
			if len(points[2].(string)) == 0 {
				points[2] = host
			}
		}
	}
	return metrics
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
			group_name, field_name := GroupMetric(name)
			group := values[group_name]
			if group == nil {
				group = make(map[string]interface{})
				values[group_name] = group
			}
			group[field_name] = value
			delete(data, key)
		}
	}
	for name, group := range values {
		metrics = append(metrics, NewMetricGroup(host, name, timestamp, group, nil))
	}

	metrics = append(metrics, mapProcesses(timestamp, data["processes"].(map[string]interface{})))
	delete(data, "processes")
	metrics = append(metrics, mapDiskMetrics("system.disk", timestamp, host, data["diskUsage"].([]interface{})))
	delete(data, "diskUsage")
	metrics = append(metrics, mapDiskMetrics("system.fs.inodes", timestamp, host, data["inodes"].([]interface{})))
	delete(data, "inodes")
	metrics = append(metrics, mapIOMetrics(timestamp, host, data["ioStats"].(map[string]interface{})))
	delete(data, "ioStats")
	metrics = append(metrics, mapExtraMetrics(host, data["metrics"].([]interface{}))...)
	delete(data, "metrics")

	debug, err := json.MarshalIndent(data, "", "  ")
	if err == nil {
		fmt.Println(string(debug))
	}
	return metrics
}

func mapDiskMetrics(name string, timestamp uint64, host string, data []interface{}) *Metric {
	columns := append([]string{"time", "hostname"}, diskMetrics...)
	points := make([][]interface{}, len(data))
	for i, disk := range data {
		fields := disk.([]interface{})
		percent := fields[4].(string)
		parse, _ := strconv.ParseFloat(percent[:len(percent)-1], 64)
		fields[4] = parse / 100.0
		points[i] = append([]interface{}{timestamp, host}, fields...)
	}
	metric := &Metric{name, columns, points}
	return metric
}

func mapIOMetrics(timestamp uint64, host string, data map[string]interface{}) *Metric {
	columns := append([]string{"time", "hostname", "device"}, ioMetrics...)
	points := [][]interface{}{}
	for device, disk := range data {
		fields := disk.(map[string]interface{})
		values := make([]interface{}, len(ioMetrics))
		for i, name := range ioMetrics {
			value := fields[ioMetricMapping[name]]
			parse, _ := strconv.ParseFloat(value.(string), 64)
			values[i] = parse
		}
		points = append(points, append([]interface{}{timestamp, host, device}, values...))
	}
	metric := &Metric{"system.io", columns, points}
	return metric
}

func GetProcessFamily(command string) string {
	if len(command) > 0 && command[0] == '[' {
		return "kernel"
	} else {
		prefix := command
		index := strings.IndexAny(command, " \t")
		if index > 0 {
			prefix = command[:index]
		}
		index = strings.LastIndex(prefix, "/")
		return prefix[index+1:]
	}
}

func mapProcesses(timestamp uint64, data map[string]interface{}) *Metric {
	host := data["host"].(string)
	processes := data["processes"].([]interface{})
	columns := append([]string{"time", "hostname", "family"}, processMetrics...)
	points := make([][]interface{}, len(processes))
	for i, process := range processes {
		fields := process.([]interface{})
		fields[1], _ = strconv.ParseInt(fields[1].(string), 10, 64)
		fields[2], _ = strconv.ParseFloat(fields[2].(string), 64)
		fields[3], _ = strconv.ParseFloat(fields[3].(string), 64)
		fields[4], _ = strconv.ParseInt(fields[4].(string), 10, 64)
		fields[5], _ = strconv.ParseInt(fields[5].(string), 10, 64)
		points[i] = append([]interface{}{timestamp, host, GetProcessFamily(fields[10].(string))}, fields...)
	}
	metric := &Metric{"processes", columns, points}
	return metric
}

func mapExtraMetrics(host string, data []interface{}) []*Metric {
	values := make(map[string]map[string]interface{})
	tags := make(map[string]map[string]string)
	timestamps := make(map[string]uint64)
	for _, tmp := range data {
		metric := tmp.([]interface{})
		name := metric[0].(string)
		timestamp := uint64(metric[1].(float64) * 1000)
		value := metric[2]

		group_name, field_name := GroupMetric(name)
		group := values[group_name]
		group_tags := tags[group_name]
		if group == nil {
			group = make(map[string]interface{})
			group_tags = make(map[string]string)
			values[group_name] = group
			tags[group_name] = group_tags
		}
		timestamps[group_name] = timestamp
		group[field_name] = value

		fields := metric[3].(map[string]interface{})
		for k, v := range fields {
			if k == "tags" {
				tags2 := v.([]interface{})
				for _, tag := range tags2 {
					split := strings.SplitN(tag.(string), ":", 2)
					group_tags[split[0]] = split[1]
				}
			} else {
				group_tags[k] = v.(string)
			}
		}
	}
	metrics := []*Metric{}
	for name, group := range values {
		metrics = append(metrics, NewMetricGroup(host, name, timestamps[name], group, tags[name]))
	}
	return metrics
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
	}

	series := StatsdSeries{}
	err = json.Unmarshal(buf.Bytes(), &series)
	if err != nil {
		log.Println(err)
		io.WriteString(w, `{"status":"failed"}`)
		return
	}

	metrics := mapStatsd(series.Series)
	go PushMetrics(metrics)

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
