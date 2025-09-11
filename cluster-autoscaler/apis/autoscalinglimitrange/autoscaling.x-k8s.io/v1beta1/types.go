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

package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +kubebuilder:storageversions

// AutoscalingLimitRange is the Schema for the autoscalinglimitranges API
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
type AutoscalingLimitRange struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AutoscalingLimitRangeSpec   `json:"spec,omitempty"`
	Status AutoscalingLimitRangeStatus `json:"status,omitempty"`
}

// AutoscalingLimitRangeSpec defines the desired state of AutoscalingLimitRange
type AutoscalingLimitRangeSpec struct {
	// Limits are the limits that are enforced.
	Limits LimitRange `json:"limits"`
	// ScopeSelector is the map of labels defining the scope the limits apply to. Usage will be calculated from the nodes matching that selector.
	ScopeSelector map[string]string `json:"scopeSelector"`
}

// LimitRange defines a min/max usage limit for a particular resource that is tracked by CA.
type LimitRange struct {
	// Max usage constraints on this kind by resource name.
	// +optional
	Max ResourceList `json:"max,omitempty"`
	// Min usage constraints on this kind by resource name.
	// +optional
	Min ResourceList `json:"min,omitempty"`
}

// AutoscalingLimitRangeStatus defines the observed state of AutoscalingLimitRange
type AutoscalingLimitRangeStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// AutoscalingLimitRangeList contains a list of AutoscalingLimitRange
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type AutoscalingLimitRangeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AutoscalingLimitRange `json:"items"`
}

type ResourceList map[string]resource.Quantity
