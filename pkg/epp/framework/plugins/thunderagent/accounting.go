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

package thunderagent

import (
	"context"
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// PreRequest charges the request's estimated prompt tokens to its session,
// binds the session to the picked endpoint, and stashes the estimate on the
// request for ResponseBody to remove. The director guarantees ResponseBody
// runs (with EndOfStream) for every request that picked a pod, including
// aborted ones, so the estimate cannot leak. A pod the ledger does not know
// (it reports no KV capacity) leaves the request untracked.
func (a *ThunderAgent) PreRequest(_ context.Context, request *fwksched.InferenceRequest, schedulingResult *fwksched.SchedulingResult) error {
	id := programID(request)
	if id == "" {
		return nil
	}
	endpoint := pickedEndpoint(schedulingResult)
	if endpoint == nil {
		return nil
	}
	md := endpoint.GetMetadata()
	if md == nil {
		return nil
	}

	m := a.mgr
	m.mu.Lock()
	pod, ok := m.endpoints[md.ID.String()]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	estimate := m.estimateTokensLocked(request.RequestSizeBytes)
	s := m.bindLocked(id, pod)
	s.paused = false
	s.reservedUntil = time.Time{}
	s.inflightTokens += estimate
	s.dispatched = true
	m.mu.Unlock()

	request.PutAttribute(inflightEstimateKey, estimate)
	return nil
}

// ResponseBody settles a completed turn: it returns the in-flight estimate,
// replaces the session's committed footprint with the usage-reported total,
// and refines the size-to-token estimator. Intermediate streaming chunks are
// ignored. Session state is released by the idle TTL only; there is no
// explicit end-of-session signal.
func (a *ThunderAgent) ResponseBody(_ context.Context, request *fwksched.InferenceRequest, response *fwkrc.Response, _ *datalayer.EndpointMetadata) {
	if request == nil || response == nil || !response.EndOfStream {
		return
	}
	id := programID(request)
	if id == "" {
		return
	}
	estimate, ok := fwksched.ReadRequestAttribute[int64](request, inflightEstimateKey)
	if !ok {
		return // PreRequest did not track this request
	}

	m := a.mgr
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refineEstimatorLocked(request.RequestSizeBytes, response.Usage.PromptTokens)
	s, ok := m.sessions[id]
	if !ok {
		return
	}
	s.inflightTokens -= estimate
	if s.inflightTokens < 0 {
		s.inflightTokens = 0
	}
	switch {
	case response.Usage.TotalTokens > 0:
		s.committedTokens = int64(response.Usage.TotalTokens)
	case estimate > s.committedTokens:
		s.committedTokens = estimate
	}
	s.lastResponseAt = time.Now()
}

// pickedEndpoint returns the endpoint the primary profile picked, or nil
// when none was scheduled.
func pickedEndpoint(schedulingResult *fwksched.SchedulingResult) fwksched.Endpoint {
	if schedulingResult == nil {
		return nil
	}
	result, ok := schedulingResult.ProfileResults[schedulingResult.PrimaryProfileName]
	if !ok || result == nil || len(result.TargetEndpoints) == 0 {
		return nil
	}
	return result.TargetEndpoints[0]
}
