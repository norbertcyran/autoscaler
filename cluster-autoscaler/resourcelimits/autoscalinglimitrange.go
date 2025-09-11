/*
Copyright 2025 The Kubernetes Authors.

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

package resourcelimits

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/apis/autoscalinglimitrange/autoscaling.x-k8s.io/v1beta1"
	listers "k8s.io/autoscaler/cluster-autoscaler/apis/autoscalinglimitrange/client/listers/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/klog/v2"
)

// AutoscalingLimitRangeLimiter is a limiter that is based on AutoscalingLimitRange CRD.
type AutoscalingLimitRangeLimiter struct {
	id        string
	minLimits map[string]int64
	maxLimits map[string]int64
	selector  labels.Selector
}

func (l *AutoscalingLimitRangeLimiter) ID() string {
	return l.id
}

// NewAutoscalingLimitRangeLimiter creates a new AutoscalingLimitRangeLimiter from an AutoscalingLimitRange.
func NewAutoscalingLimitRangeLimiter(alr *v1beta1.AutoscalingLimitRange) (*AutoscalingLimitRangeLimiter, error) {
	selector, err := labels.ValidatedSelectorFromSet(alr.Spec.ScopeSelector)
	if err != nil {
		return nil, err
	}

	minLimits := make(map[string]int64)
	for resource, quantity := range alr.Spec.Limits.Min {
		minLimits[resource] = quantity.Value()
	}

	maxLimits := make(map[string]int64)
	for resource, quantity := range alr.Spec.Limits.Max {
		maxLimits[resource] = quantity.Value()
	}

	return &AutoscalingLimitRangeLimiter{
		id:        fmt.Sprintf("%s/%s", alr.Kind, alr.Name),
		minLimits: minLimits,
		maxLimits: maxLimits,
		selector:  selector,
	}, nil
}

// MatchesNode checks if the limiter applies to a given node.
func (l *AutoscalingLimitRangeLimiter) MatchesNode(node *apiv1.Node) bool {
	return l.selector.Matches(labels.Set(node.GetLabels()))
}

// MaxLimits returns max limits.
func (l *AutoscalingLimitRangeLimiter) MaxLimits() map[string]int64 {
	return l.maxLimits
}

// MinLimits returns min limits.
func (l *AutoscalingLimitRangeLimiter) MinLimits() map[string]int64 {
	return l.minLimits
}

// AutoscalingLimitRangeProvider is a provider that gets resource limits from AutoscalingLimitRange CRDs.
type AutoscalingLimitRangeProvider struct {
	lister listers.AutoscalingLimitRangeLister
}

// NewAutoscalingLimitRangeProvider creates a new AutoscalingLimitRangeProvider.
func NewAutoscalingLimitRangeProvider(lister listers.AutoscalingLimitRangeLister) *AutoscalingLimitRangeProvider {
	return &AutoscalingLimitRangeProvider{
		lister: lister,
	}
}

func (p *AutoscalingLimitRangeProvider) AllLimiters() ([]Limiter, error) {
	allRanges, err := p.lister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var limiters []Limiter
	for _, alr := range allRanges {
		if limiter, err := NewAutoscalingLimitRangeLimiter(alr); err == nil {
			limiters = append(limiters, limiter)
		} else {
			klog.Warningf("Failed to process AutoscalingLimitRange %s/%s: %v", alr.Namespace, alr.Name, err)
		}
	}
	return limiters, nil

}

// ResourceLimitersForNode returns limiters for a given node.
func (p *AutoscalingLimitRangeProvider) ResourceLimitersForNode(node *apiv1.Node) ([]Limiter, error) {
	allRanges, err := p.lister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var limiters []Limiter
	for _, alr := range allRanges {
		if limiter, err := NewAutoscalingLimitRangeLimiter(alr); err == nil {
			if limiter.MatchesNode(node) {
				limiters = append(limiters, limiter)
			}
		} else {
			klog.Warningf("Failed to process AutoscalingLimitRange %s/%s: %v", alr.Namespace, alr.Name, err)
		}
	}

	return limiters, nil
}
