// Copyright 2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	LblType       = "type"
	LblStatusType = "status"
	LblRuleIDType = "rule"
	LblOpIDType   = "op"
	LblIOType     = "io"

	LBlRuleRunning = "running"
	LblRuleStop    = "stop"
	LblSourceIO    = "source"
	LblSinkIO      = "sink"
	LblException   = "err"
	LblSuccess     = "success"
)

var (
	SyncCacheDurationHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kuiper",
		Subsystem: "sync_cache",
		Name:      "duration",
		Buckets:   prometheus.ExponentialBuckets(10, 2, 20), // 10us ~ 5s
		Help:      "hist of sync cache",
	}, []string{LblType, LblRuleIDType, LblOpIDType})

	SyncCacheOpCnter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kuiper",
		Subsystem: "sync_cache",
		Name:      "counter",
		Help:      "counter of sync cache",
	}, []string{LblType, LblRuleIDType, LblOpIDType})

	SyncCacheGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kuiper",
		Subsystem: "sync_cache",
		Name:      "gauge", // 10us ~ 5s
		Help:      "gauge of sync cache",
	}, []string{LblType, LblRuleIDType, LblOpIDType})
)

func RegisterSyncCacheMetrics() {
	prometheus.MustRegister(SyncCacheOpCnter)
	prometheus.MustRegister(SyncCacheDurationHist)
	prometheus.MustRegister(SyncCacheGauge)
}
