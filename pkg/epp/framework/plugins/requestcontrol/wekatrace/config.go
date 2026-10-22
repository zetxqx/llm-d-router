/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wekatrace

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Mode values.
const (
	ModeDogfood  = "dogfood"
	ModeCustomer = "customer"
)

const (
	defaultSampleRate        = 0.01
	defaultSinkPath          = "/var/weka-trace"
	defaultFileBytes         = 64 << 20
	defaultDiskHighWatermark = 900 << 20
	defaultQueueBytes        = 64 << 20
	defaultLRUBytes          = 64 << 20
	// customerArrivalResolutionMs coarsens arrival times in customer mode to
	// make joins against trace backends by timestamp less precise.
	customerArrivalResolutionMs = 100
)

// Config is the plugin's parameter block.
type Config struct {
	// Mode is required: "dogfood" or "customer".
	Mode string `json:"mode"`
	// Consent must be true when Mode is "customer".
	Consent bool `json:"consent"`
	// KeySecretPath is a file holding the PRF key. A missing or unreadable file
	// disables the recorder at runtime without affecting requests.
	KeySecretPath string `json:"keySecretPath"`
	// KeyID labels the key epoch in every record.
	KeyID string `json:"keyID"`
	// SampleRate is the fraction of sessions recorded.
	SampleRate *float64 `json:"sampleRate,omitempty"`
	// AdditionalSessionHeaders are checked before the agent-identity defaults.
	AdditionalSessionHeaders []string `json:"additionalSessionHeaders,omitempty"`
	// ModelAllowlist lists model names written verbatim; others are pseudonymized.
	ModelAllowlist []string   `json:"modelAllowlist,omitempty"`
	Sink           SinkConfig `json:"sink"`
	// QueueMaxBytes bounds pending work, including referenced token slices.
	QueueMaxBytes *resource.Quantity `json:"queueMaxBytes,omitempty"`
	// LRUMaxBytes bounds the per-session delta state.
	LRUMaxBytes *resource.Quantity `json:"lruMaxBytes,omitempty"`
}

// SinkConfig configures the file sink.
type SinkConfig struct {
	Path              string             `json:"path,omitempty"`
	FileBytes         *resource.Quantity `json:"fileBytes,omitempty"`
	DiskHighWatermark *resource.Quantity `json:"diskHighWatermark,omitempty"`
	// Stdout additionally writes every record to stdout. Dogfood only.
	Stdout bool `json:"stdout,omitempty"`
}

type resolvedConfig struct {
	mode                string
	keySecretPath       string
	keyID               string
	sampleRate          float64
	sessionHeaders      []string
	modelAllowlist      map[string]struct{}
	sinkPath            string
	fileBytes           int64
	diskHighWatermark   int64
	stdout              bool
	queueBytes          int64
	lruBytes            int64
	arrivalResolutionMs int64
}

func quantityOr(q *resource.Quantity, def int64) int64 {
	if q == nil {
		return def
	}
	return q.Value()
}

// resolve validates c. Errors here fail config loading, so they are reserved
// for misconfiguration that must be visible at deploy time.
func (c Config) resolve() (resolvedConfig, error) {
	switch c.Mode {
	case ModeDogfood, ModeCustomer:
	case "":
		return resolvedConfig{}, errors.New("mode is required (dogfood or customer)")
	default:
		return resolvedConfig{}, fmt.Errorf("unknown mode %q", c.Mode)
	}
	if c.Mode == ModeCustomer && !c.Consent {
		return resolvedConfig{}, errors.New("mode customer requires consent: true")
	}
	if c.Mode == ModeCustomer && c.Sink.Stdout {
		return resolvedConfig{}, errors.New("sink.stdout is not allowed in mode customer")
	}
	if c.KeySecretPath == "" {
		return resolvedConfig{}, errors.New("keySecretPath is required")
	}
	rate := defaultSampleRate
	if c.SampleRate != nil {
		rate = *c.SampleRate
	}
	if rate < 0 || rate > 1 {
		return resolvedConfig{}, fmt.Errorf("sampleRate %v is outside [0, 1]", rate)
	}
	r := resolvedConfig{
		mode:              c.Mode,
		keySecretPath:     c.KeySecretPath,
		keyID:             c.KeyID,
		sampleRate:        rate,
		sessionHeaders:    sessionHeaders(c.AdditionalSessionHeaders),
		modelAllowlist:    make(map[string]struct{}, len(c.ModelAllowlist)),
		sinkPath:          c.Sink.Path,
		fileBytes:         quantityOr(c.Sink.FileBytes, defaultFileBytes),
		diskHighWatermark: quantityOr(c.Sink.DiskHighWatermark, defaultDiskHighWatermark),
		stdout:            c.Sink.Stdout,
		queueBytes:        quantityOr(c.QueueMaxBytes, defaultQueueBytes),
		lruBytes:          quantityOr(c.LRUMaxBytes, defaultLRUBytes),
	}
	if r.sinkPath == "" {
		r.sinkPath = defaultSinkPath
	}
	for _, m := range c.ModelAllowlist {
		r.modelAllowlist[m] = struct{}{}
	}
	if c.Mode == ModeCustomer {
		r.arrivalResolutionMs = customerArrivalResolutionMs
	}
	if r.fileBytes <= 0 || r.queueBytes <= 0 || r.lruBytes <= 0 || r.diskHighWatermark <= 0 {
		return resolvedConfig{}, errors.New("byte limits must be positive")
	}
	return r, nil
}
