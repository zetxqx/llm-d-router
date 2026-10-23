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

// Command weka-export converts weka-trace-recorder JSONL shape records into
// WekaTrace v7 files loadable by aiperf.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/pflag"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/wekatrace/export"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "weka-export:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	var opts export.Options
	fs := pflag.NewFlagSet("weka-export", pflag.ContinueOnError)
	in := fs.String("in", "", "Directory with recorder *.jsonl files.")
	out := fs.String("out", "", "Directory to write one <trace-id>.json WekaTrace file per trace.")
	sidecar := fs.String("sidecar", "", "Path of the per-request sidecar JSONL. Empty skips it.")
	fs.BoolVar(&opts.AllowEstimate, "allow-estimate", false, "Export records whose block ids come from an approximate tokenizer.")
	fs.DurationVar(&opts.SkewTol, "skew-tol", export.DefaultSkewTol, "Clock skew tolerance between replicas for lineage reconstruction.")
	fs.StringVar(&opts.Seed, "seed", "", "Derive trace ids deterministically from this seed. Empty uses random ids.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *in == "" || *out == "" {
		return errors.New("--in and --out are required")
	}
	if opts.SkewTol < 0 {
		return errors.New("--skew-tol must not be negative")
	}

	res, err := export.Export(*in, opts)
	if err != nil {
		return err
	}
	if err := export.WriteTraces(*out, res.Traces); err != nil {
		return err
	}
	if *sidecar != "" {
		if err := export.WriteSidecar(*sidecar, res.Sidecar); err != nil {
			return err
		}
	}
	fmt.Printf("weka-export: traces=%d requests=%d %+v\n", len(res.Traces), len(res.Sidecar), res.Stats)
	return nil
}
