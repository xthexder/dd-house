package main

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"strings"
)

var (
	listenAddr    string
	eventLogPath  string
	processFilter float64
	authApiKey    string
	dbUrl         string
	seriesUrl     string
	dbName        string

	eventLog   *os.File
	eventsChan chan []byte
)

var rootMetrics = map[string]string{
	"agentVersion": "host.meta.agent_version",
	"os":           "host.meta.os",
	"python":       "host.meta.python_version",
	"uuid":         "host.meta.uuid",

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
var aggregateProcessMetrics = []string{
	"user",
	"pid",
	"pct_cpu",
	"pct_mem",
	"vsz",
	"rss",
	"family",
	"command",
	"ps_count",
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

func NewMetricGroup(host, name string, timestamp uint64, values map[string]interface{}, tags map[string]interface{}) *Metric {
	columns := []string{"time", "hostname"}
	points := [][]interface{}{{timestamp, host}}
	if values != nil {
		for k, v := range values {
			if k == "time" || k == "hostname" {
				columns = append(columns, "_"+k)
			} else {
				columns = append(columns, k)
			}
			points[0] = append(points[0], v)
		}
	}

	if tags != nil {
		for k, v := range tags {
			if k == "hostname" {
				points[0][1] = v
			} else {
				if k == "time" {
					columns = append(columns, "_"+k)
				} else {
					columns = append(columns, k)
				}
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

	resp, err := http.Post(seriesUrl, "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Println(err)
	} else if resp.StatusCode != 200 {
		dump, _ := httputil.DumpResponse(resp, true)
		log.Printf("Got Response: %s\n%s\n\n\n%s", resp.Status, string(body), string(dump))
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
					if split[0] == "time" || split[0] == "value" {
						columns = append(columns, "_"+split[0])
					} else {
						columns = append(columns, split[0])
					}
					for i, _ := range points {
						points[i] = append(points[i], split[1])
					}
				}
			}
		}
		metrics[i] = &Metric{"statsd." + metric.Metric, columns, points}
	}

	log.Printf("Parsed statsd for: %s\n", host)

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
	host := data["internalHostname"].(string)
	log.Printf("Parsing metrics for: %s\n", host)

	delete(data, "apiKey")
	delete(data, "internalHostname")

	if data["events"] != nil {
		parseEvents(data["events"].(map[string]interface{}))
		delete(data, "events")
	}

	metrics := []*Metric{}
	if data["collection_timestamp"] != nil {
		timestamp := uint64(data["collection_timestamp"].(float64) * 1000)
		delete(data, "collection_timestamp")

		metrics = append(metrics, mapMetadata(host, timestamp, data)...)
		values := make(map[string]map[string]interface{})

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

		agentChecks, ok := data["agent_checks"]
		if ok {
			metrics = append(metrics, mapAgentChecks(host, timestamp, agentChecks.([]interface{}))...)
			delete(data, "agent_checks")
		}

		metrics = append(metrics, mapProcesses(timestamp, data["processes"].(map[string]interface{})))
		delete(data, "processes")
		delete(data, "resources") // Only ever contains process data that is already collected above
		metrics = append(metrics, mapDiskMetrics("system.disk", host, timestamp, data["diskUsage"].([]interface{})))
		delete(data, "diskUsage")
		metrics = append(metrics, mapDiskMetrics("system.fs.inodes", host, timestamp, data["inodes"].([]interface{})))
		delete(data, "inodes")
		metrics = append(metrics, mapIOMetrics(host, timestamp, data["ioStats"].(map[string]interface{})))
		delete(data, "ioStats")
	} else {
		delete(data, "uuid")
	}
	if data["service_checks"] != nil {
		metrics = append(metrics, mapServiceChecks(data["service_checks"].([]interface{}))...)
		delete(data, "service_checks")
	}
	if data["metrics"] != nil {
		metrics = append(metrics, mapExtraMetrics(host, data["metrics"].([]interface{}))...)
		delete(data, "metrics")
	}

	if len(data) > 0 {
		debug, err := json.MarshalIndent(data, "", "  ")
		if err == nil {
			fmt.Println("Unprocessed metrics:", string(debug))
		}
	}
	return metrics
}

func mapMetadata(host string, timestamp uint64, data map[string]interface{}) []*Metric {
	metrics := []*Metric{}
	meta, ok := data["meta"]
	if ok {
		metrics = append(metrics, NewMetricGroup(host, "host.meta.hostnames", timestamp, nil, meta.(map[string]interface{})))

		hostTags := data["host-tags"].(map[string]interface{})
		for k, v := range hostTags {
			tmp := v.([]interface{})
			tags := make([]string, len(tmp))
			for i, tag := range tmp {
				tags[i] = tag.(string)
			}
			hostTags[k] = strings.Join(tags, ",")
		}
		if len(hostTags) > 0 {
			metrics = append(metrics, NewMetricGroup(host, "host.meta.tags", timestamp, nil, hostTags))
		}

		systemStats := data["systemStats"].(map[string]interface{})
		for k, v := range systemStats {
			tmp, ok := v.([]interface{})
			if ok {
				parts := make([]string, len(tmp))
				for i, part := range tmp {
					parts[i] = part.(string)
				}
				systemStats[k] = strings.Join(parts, "-")
			}
		}
		metrics = append(metrics, NewMetricGroup(host, "host.meta.stats", timestamp, systemStats, nil))

		delete(data, "meta")
		delete(data, "systemStats")
	}
	delete(data, "host-tags")

	return metrics
}

func mapServiceChecks(data []interface{}) []*Metric {
	metrics := []*Metric{}
	for _, check := range data {
		values := check.(map[string]interface{})
		host := values["host_name"].(string)
		name := "service." + values["check"].(string)
		timestamp := uint64(values["timestamp"].(float64) * 1000)
		var tags map[string]interface{}
		if values["tags"] != nil {
			tags = values["tags"].(map[string]interface{})
		}
		delete(values, "check")
		delete(values, "tags")
		delete(values, "host_name")
		delete(values, "timestamp")
		metrics = append(metrics, NewMetricGroup(host, name, timestamp, values, tags))
	}
	return metrics
}

func mapAgentChecks(host string, timestamp uint64, data []interface{}) []*Metric {
	metrics := []*Metric{}
	for _, check := range data {
		values := check.([]interface{})
		name := "check."
		if values[1] == nil {
			name += values[0].(string)
		} else {
			name += values[1].(string) + "." + values[0].(string)
		}
		new_values := make(map[string]interface{})
		new_values["instance_id"] = values[2]
		new_values["status"] = values[3]
		message := ""
		messages, ok := values[4].([]interface{})
		if ok {
			for _, msg := range messages {
				if len(message) > 0 {
					message += "\n"
				}
				message += msg.(string)
			}
		} else {
			message = values[4].(string)
		}
		new_values["message"] = message
		metrics = append(metrics, NewMetricGroup(host, name, timestamp, new_values, nil))
	}
	return metrics
}

func mapDiskMetrics(name string, host string, timestamp uint64, data []interface{}) *Metric {
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

func mapIOMetrics(host string, timestamp uint64, data map[string]interface{}) *Metric {
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
	prefix := command
	index := strings.IndexAny(command, " \t")
	if index > 0 {
		prefix = command[:index]
	}
	index = strings.LastIndex(prefix, "/")
	return prefix[index+1:]
}

func mapProcesses(timestamp uint64, data map[string]interface{}) *Metric {
	host := data["host"].(string)
	processes := data["processes"].([]interface{})
	aggregate := make(map[string][]interface{})
	for _, process := range processes {
		fields := process.([]interface{})
		family := "kernel"
		aggr := "kernel"

		command := fields[10].(string)
		if len(command) == 0 || command[0] != '[' {
			family = GetProcessFamily(command)
			aggr = command
		}

		result, ok := aggregate[aggr]
		if !ok {
			result = make([]interface{}, len(aggregateProcessMetrics))
			result[8] = 0
			aggregate[aggr] = result
		}

		result[0] = fields[0].(string)
		result[1], _ = strconv.ParseInt(fields[1].(string), 10, 64)
		result[2], _ = strconv.ParseFloat(fields[2].(string), 64)
		result[3], _ = strconv.ParseFloat(fields[3].(string), 64)
		result[4], _ = strconv.ParseInt(fields[4].(string), 10, 64)
		result[5], _ = strconv.ParseInt(fields[5].(string), 10, 64)
		result[6] = family
		result[7] = command
		result[8] = result[8].(int) + 1
	}

	columns := append([]string{"time", "hostname"}, aggregateProcessMetrics...)
	points := [][]interface{}{}
	for _, process := range aggregate {
		if process[2].(float64) >= processFilter || process[3].(float64) >= processFilter {
			points = append(points, append([]interface{}{timestamp, host}, process...))
		}
	}
	metric := &Metric{"processes", columns, points}
	return metric
}

type ExtraMetric struct {
	Values    []interface{}
	Tags      []map[string]interface{}
	Timestamp uint64
}

func (self *ExtraMetric) ToMetric(host, name string) *Metric {
	columns := []string{"time", "value", "hostname"}
	points := make([][]interface{}, len(self.Values))
	for i, value := range self.Values {
		points[i] = []interface{}{self.Timestamp, value, host}
	}

	for i, tags := range self.Tags {
		for k, v := range tags {
			if k == "time" || k == "value" {
				k = "_" + k
			}
			found := false
			for j, column := range columns {
				if column == k {
					points[i][j] = v
					found = true
					break
				}
			}
			if !found {
				columns = append(columns, k)
				for j, _ := range points {
					if j == i {
						points[j] = append(points[j], v)
					} else {
						points[j] = append(points[j], nil)
					}
				}
			}
		}
	}
	return &Metric{name, columns, points}
}

func addToExtraMetric(metric *ExtraMetric, value interface{}, tags map[string]interface{}) {
	metric.Values = append(metric.Values, value)
	for k, v := range tags {
		if k == "tags" {
			tags2 := v.([]interface{})
			for _, tag := range tags2 {
				split := strings.SplitN(tag.(string), ":", 2)
				tags[split[0]] = split[1]
			}
			delete(tags, "tags")
		}
	}
	metric.Tags = append(metric.Tags, tags)
}

func mapExtraMetrics(host string, data []interface{}) []*Metric {
	groups := make(map[string]map[string]*ExtraMetric)

	for _, tmp := range data {
		metric := tmp.([]interface{})
		name := metric[0].(string)
		timestamp := uint64(metric[1].(float64) * 1000)

		group_name, field_name := GroupMetric(name)
		group, ok := groups[group_name]
		if !ok {
			group = make(map[string]*ExtraMetric)
			groups[group_name] = group
		}
		extraMetric, ok := group[field_name]
		if !ok {
			extraMetric = &ExtraMetric{[]interface{}{}, []map[string]interface{}{}, timestamp}
			group[field_name] = extraMetric
		}
		addToExtraMetric(extraMetric, metric[2], metric[3].(map[string]interface{}))
	}

	metrics := []*Metric{}
	for group_name, group := range groups {
		groupValues := make(map[string]interface{})
		groupTags := make(map[string]interface{})
		var groupTimestamp uint64
		for field_name, extraMetric := range group {
			if len(extraMetric.Values) > 1 {
				metrics = append(metrics, extraMetric.ToMetric(host, group_name+"."+field_name))
			} else {
				groupValues[field_name] = extraMetric.Values[0]
				for k, v := range extraMetric.Tags[0] {
					groupTags[k] = v
				}
				groupTimestamp = extraMetric.Timestamp
			}
		}
		if len(groupValues) > 0 {
			metrics = append(metrics, NewMetricGroup(host, group_name, groupTimestamp, groupValues, groupTags))
		}
	}
	return metrics
}

func parseEvents(data map[string]interface{}) {
	for source, tmp := range data {
		events := tmp.([]interface{})
		for _, tmp2 := range events {
			event := tmp2.(map[string]interface{})
			event["source"] = source

			buf, err := json.Marshal(event)
			if err != nil {
				log.Println("Failed to marshal event:", err)
				continue
			}
			eventsChan <- buf
		}
	}
}

func handleIntake(w http.ResponseWriter, req *http.Request) {
	if handleApiKey(w, req) {
		return
	}

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

func writeEvents() {
	defer func() {
		eventLog.Close()
	}()

	for event := range eventsChan {
		_, err := eventLog.Write(append(event, '\n'))
		if err != nil {
			log.Println("Failed to write event:", string(event), err)
		}
	}
}

func CreateDBIfNotExists() error {
	log.Println("Checking if DB exists:", dbName)

	resp, err := http.Get(dbUrl)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer([]byte{})
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return err
	}
	response := []map[string]string{}
	err = json.Unmarshal(buf.Bytes(), &response)
	if err != nil {
		return err
	}

	for _, db := range response {
		if db["name"] == dbName {
			return nil
		}
	}

	log.Printf("Creating DB: %s\n", dbName)

	body, err := json.Marshal(map[string]string{"name": dbName})
	if err != nil {
		return err
	}

	resp, err = http.Post(dbUrl, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	} else if resp.StatusCode != 201 {
		dump, _ := httputil.DumpResponse(resp, true)
		log.Printf("Got Response: %s\n%s\n\n\n%s", resp.Status, string(body), string(dump))
	}

	return nil
}

func main() {
	listenAddr = os.Getenv("ADDR")
	if len(listenAddr) == 0 {
		listenAddr = ":8080"
	}
	eventLogPath = os.Getenv("EVENT_LOG")
	if len(eventLogPath) == 0 {
		eventLogPath = "events.log"
	}
	processFilterString := os.Getenv("PS_FILTER")
	if len(processFilterString) == 0 {
		processFilter = 0.1
	} else {
		var err error
		processFilter, err = strconv.ParseFloat(processFilterString, 64)
		if err != nil {
			log.Panicln(err)
		}
	}

	authApiKey = os.Getenv("API_KEY")
	if len(authApiKey) == 0 {
		log.Println("Warning: API_KEY is blank, any key is accepted")
	}
	inputUrl := os.Getenv("DB_URL")
	if len(inputUrl) == 0 {
		seriesUrl = "http://localhost:8086/db/datadog/series?u=root&p=root"
		dbUrl = "http://localhost:8086/db?u=root&p=root"
		dbName = "datadog"
	} else {
		split := strings.Split(inputUrl, "/")
		split2 := strings.SplitN(split[4], "?", 2)
		dbName = split2[0]
		seriesUrl = strings.Join(split[0:4], "/") + "/" + dbName + "/series?" + split2[1]
		dbUrl = strings.Join(split[0:4], "/") + "?" + split2[1]
	}
	err := CreateDBIfNotExists()
	if err != nil {
		log.Panicln(err)
	}

	eventLog, err = os.OpenFile(eventLogPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		log.Panicln(err)
	}
	eventsChan = make(chan []byte, 5)

	go writeEvents()

	log.Println("dd-house listening on", listenAddr)

	http.HandleFunc("/intake", handleIntake)
	http.HandleFunc("/api/v1/series/", handleApi)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
