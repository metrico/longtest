package main

import (
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type PromReq []prompb.TimeSeries

func (p PromReq) Serialize() ([]byte, error) {
	bytes, err := proto.Marshal(&prompb.WriteRequest{Timeseries: p})
	if err != nil {
		return nil, err
	}
	enc := snappy.Encode(nil, bytes)
	return enc, nil
}

// histogramState holds the data for a single histogram metric.
type histogramState struct {
	buckets map[float64]uint64
	sum     float64
	count   uint64
}

func NewMetricSender(opts LogSenderOpts) ISender {
	var l *GenericSender
	hdrs := opts.Headers
	opts.Headers = map[string]string{}
	for k, v := range hdrs {
		opts.Headers[k] = v
	}
	opts.Headers["Content-Type"] = "application/x-protobuf"
	opts.Headers["Content-Encoding"] = "snappy"

	// Initialize state for counters and histograms within the sender instance.
	counters := make(map[string]float64)
	histograms := make(map[string]*histogramState)

	l = &GenericSender{
		LogSenderOpts: opts,
		mtx:           sync.Mutex{},
		rnd:           rand.New(rand.NewSource(time.Now().UnixNano())),
		timeout:       time.Second * 15,
		path:          "/api/v1/prom/remote/write",
		generate: func() IRequest {
			// Each generation cycle will produce 3 gauges, 1 counter, 1 histogram (5 series), and 1 summary (5 series).
			// Total series per container = 3 + 1 + 5 + 5 = 14
			req := make(PromReq, 0, len(l.Containers)*14)
			now := time.Now().UnixMilli()
			for _, container := range l.Containers {
				base := int(crc32.ChecksumIEEE([]byte(container)))
				orgID := opts.Headers["X-Scope-OrgID"]
				baseLabels := []prompb.Label{
					{Name: "container", Value: container},
					{Name: "orgid", Value: orgID},
					{Name: "sender", Value: "logmetrics"},
				}
				req = append(req,
					prompb.TimeSeries{
						Labels:  append(baseLabels, prompb.Label{Name: "__name__", Value: "cpu_usage"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: math.Max(float64(base%100+(l.random(20)-10)), 0)}},
					},
					prompb.TimeSeries{
						Labels:  append(baseLabels, prompb.Label{Name: "__name__", Value: "ram_usage"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: math.Max(float64(base%1000+(l.random(200)-100)), 0)}},
					},
					prompb.TimeSeries{
						Labels:  append(baseLabels, prompb.Label{Name: "__name__", Value: "network_usage"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: math.Max(float64(base%1000000+(l.random(2000)-1000)), 0)}},
					},
				)

				counterName := "http_requests_total"
				counters[container] += float64(l.random(5) + 1) // Increment by a random amount
				req = append(req, prompb.TimeSeries{
					Labels:  append(baseLabels, prompb.Label{Name: "__name__", Value: counterName}),
					Samples: []prompb.Sample{{Timestamp: now, Value: counters[container]}},
				})

				histName := "request_latency_seconds"
				if _, ok := histograms[container]; !ok {
					histograms[container] = &histogramState{
						buckets: map[float64]uint64{0.1: 0, 0.5: 0, 1: 0, 5: 0},
					}
				}
				hState := histograms[container]
				observedLatency := math.Abs(l.rnd.Float64() * 2) // 0 to 2 seconds
				hState.sum += observedLatency
				hState.count++
				for bucketLe := range hState.buckets {
					if observedLatency <= bucketLe {
						hState.buckets[bucketLe]++
					}
				}

				histLabels := append(baseLabels, prompb.Label{Name: "__name__", Value: histName})
				bucketKeys := make([]float64, 0, len(hState.buckets))
				for k := range hState.buckets {
					bucketKeys = append(bucketKeys, k)
				}
				sort.Float64s(bucketKeys)

				for _, le := range bucketKeys {
					bucketLabels := append(histLabels, prompb.Label{Name: "le", Value: fmt.Sprintf("%v", le)})
					req = append(req, prompb.TimeSeries{
						Labels:  bucketLabels,
						Samples: []prompb.Sample{{Timestamp: now, Value: float64(hState.buckets[le])}},
					})
				}
				// Add +Inf bucket (same value as count)
				req = append(req, prompb.TimeSeries{
					Labels:  append(histLabels, prompb.Label{Name: "le", Value: "+Inf"}),
					Samples: []prompb.Sample{{Timestamp: now, Value: float64(hState.count)}},
				})
				// Add _sum and _count series
				req = append(req,
					prompb.TimeSeries{
						Labels:  append(histLabels, prompb.Label{Name: "__name__", Value: histName + "_sum"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: hState.sum}},
					},
					prompb.TimeSeries{
						Labels:  append(histLabels, prompb.Label{Name: "__name__", Value: histName + "_count"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: float64(hState.count)}},
					},
				)

				summaryName := "response_size_bytes"
				summaryLabels := append(baseLabels, prompb.Label{Name: "__name__", Value: summaryName})

				p50 := float64(base%1000 + l.random(500))
				p90 := p50 * (1.5 + l.rnd.Float64())
				p99 := p90 * (1.2 + l.rnd.Float64())

				req = append(req,
					prompb.TimeSeries{ // P50
						Labels:  append(summaryLabels, prompb.Label{Name: "quantile", Value: "0.5"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: p50}},
					},
					prompb.TimeSeries{ // P90
						Labels:  append(summaryLabels, prompb.Label{Name: "quantile", Value: "0.9"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: p90}},
					},
					prompb.TimeSeries{ // P99
						Labels:  append(summaryLabels, prompb.Label{Name: "quantile", Value: "0.99"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: p99}},
					},
					prompb.TimeSeries{ // Summary Sum
						Labels:  append(baseLabels, prompb.Label{Name: "__name__", Value: summaryName + "_sum"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: p99 * 10}},
					},
					prompb.TimeSeries{ // Summary Count
						Labels:  append(baseLabels, prompb.Label{Name: "__name__", Value: summaryName + "_count"}),
						Samples: []prompb.Sample{{Timestamp: now, Value: 100}},
					},
				)
			}
			return req
		},
	}
	return l
}
