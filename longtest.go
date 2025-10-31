package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	kind := os.Getenv("KIND")
	if kind == "" {
		kind = "WRITE"
	}
	switch kind {
	case "WRITE":
		writeTest()
	}
}

func writeTest() {
	fmt.Println("GENERATING")
	logs := generateLogs()
	fmt.Println("SENDING")
	oids := []string{""}
	if os.Getenv("ORG_ID") != "" {
		oids = strings.Split(os.Getenv("ORG_ID"), ",")
	}
	useDrilldown := os.Getenv("DRILLDOWN")
	dsn := os.Getenv("DSN")
	extraHeaders := os.Getenv("EXTRA_HEADERS")
	for _, oid := range oids {
		names := generateNames(5)
		headers := map[string]string{}
		if oid != "" {
			headers["X-Scope-OrgID"] = oid
		}
		if dsn != "" {
			headers["X-CH-DSN"] = dsn
		}
		if extraHeaders != "" {
			headerPairs := strings.Split(extraHeaders, ";")
			for _, pair := range headerPairs {
				kv := strings.SplitN(pair, ":", 2)
				if len(kv) == 2 {
					key := strings.TrimSpace(kv[0])
					value := strings.TrimSpace(kv[1])
					if key != "" {
						headers[key] = value
					}
				}
			}
		switch useDrilldown {
		case "1", "true", "TRUE", "True":
			headers["X-Drilldown"] = "1"
		case "0", "false", "FALSE", "False":
			headers["X-Drilldown"] = "0"
		default:
			headers["X-Drilldown"] = "0"

		}
		if strings.Contains(os.Getenv("MODE"), "L") {
			fmt.Println("Run json logs test")
			sender := NewLogSender(LogSenderOpts{
				ID:         "logs",
				Containers: names,
				Lines:      logs,
				LinesPS:    1000,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			sender.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "W") {
			fmt.Println("Run websocket test")
			sender := NewWsTest(LogSenderOpts{
				ID:         "ws",
				Containers: names,
				Lines:      logs,
				LinesPS:    5,
				URL:        os.Getenv("URL"),
				ReaderURL:  os.Getenv("READER_URL"),
				Headers:    headers,
			})
			sender.Run()
		}
		// Not implemented yet
		/* if strings.Contains(os.Getenv("MODE"), "P") {
		        fmt.Println("Run logs PB")
		        _headers := make(map[string]string, 20)
		        for k, v := range headers {
		                _headers[k] = v
		        }
		        _headers["Content-Type"] = "application/x-protobuf"
		        sender := NewPBSender(LogSenderOpts{
		                ID:         "logs",
		                Containers: names,
		                Lines:      logs,
		                LinesPS:    50000,
		                URL:        os.Getenv("URL"),
		                Headers:    _headers,
		        })
		        sender.Run()
		}*/
		if strings.Contains(os.Getenv("MODE"), "M") {
			fmt.Println("Run remotewrite metrics test")
			metrics := NewMetricSender(LogSenderOpts{
				ID:         "metrics",
				Containers: names,
				Lines:      logs,
				LinesPS:    3000,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			metrics.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "Z") {
			fmt.Println("Run zipkin test")
			zipkins := NewZipkinSender(LogSenderOpts{
				ID:         "traces",
				Containers: names,
				Lines:      logs,
				LinesPS:    300,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			zipkins.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "O") {
			fmt.Println("Run otlp traces test")
			zipkins := NewOTLPSender(LogSenderOpts{
				ID:         "traces",
				Containers: names,
				Lines:      logs,
				LinesPS:    40,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			zipkins.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "D") {
			fmt.Println("Run datadog traces test")
			datadogs := NewDatadogSender(LogSenderOpts{
				ID:         "traces",
				Containers: names,
				Lines:      logs,
				LinesPS:    300,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			datadogs.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "I") {
			fmt.Println("Run influx logs test")
			influx := NewInfluxSender(LogSenderOpts{
				ID:         "influx",
				Containers: names,
				Lines:      logs,
				LinesPS:    3000,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			influx.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "C") {
			fmt.Println("Run consistency test")
			cons := NewJSONConsistencyChecker(LogSenderOpts{
				ID:         "consistency-1",
				Containers: names,
				Lines:      logs,
				LinesPS:    3000,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			cons.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "S") {
			fmt.Println("Run servicegraph test")
			pqt := NewSGSender(LogSenderOpts{
				ID:         "longtest-SG",
				Containers: names,
				Lines:      logs,
				LinesPS:    30,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			pqt.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "T") {
			fmt.Println("Run time consistency test")
			pqt := NewTimeSender(LogSenderOpts{
				ID:         "longtest-TIME",
				Containers: names,
				Lines:      logs,
				LinesPS:    10,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			pqt.Run()
		}
		if strings.Contains(os.Getenv("MODE"), "J") {
			fmt.Println("Run plain text lines test")
			plainText := NewPlainTextSender(LogSenderOpts{
				ID:         "plain-text",
				Containers: names,
				Lines:      logs,
				LinesPS:    3000,
				URL:        os.Getenv("URL"),
				Headers:    headers,
			})
			plainText.Run()
		}
	}
	t := time.NewTicker(time.Second)
	go func() {
		for range t.C {
			s := stats.Collect()
			fmt.Printf("Ok requests: %d, Errors: %d, Failed: %d\n", s[REQ_OK], s[REQ_ERR], s[REQ_FAIL])
			fmt.Printf("Ok Requests time: min: %d, max: %d, avg: %f\n",
				s[REQ_TIME_MS+"_min"],
				s[REQ_TIME_MS+"_max"],
				float64(s[REQ_TIME_MS+"_sum"])/float64(s[REQ_TIME_MS+"_count"]))
			fmt.Printf("Ok Requests MB sent: %f, (%fMB/s)\n",
				float64(s[REQ_BYTES+"_sum"])/1024/1024,
				float64(s[REQ_BYTES+"_sum"])/1024/1024/5,
			)
		}
	}()
	for {
		time.Sleep(time.Second)
	}
}
