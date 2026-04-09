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

package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/testutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPodTemplateGenerationFilter(t *testing.T) {
	podTempGen3 := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "podTempGen3",
			Namespace:  "default",
			Generation: 3,
		},
	}
	podTempGen4 := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "podTempGen4",
			Namespace:  "default",
			Generation: 4,
		},
	}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(podTempGen3, podTempGen4).Build()

	tests := []struct {
		name                       string
		buffers                    []*v1.CapacityBuffer
		expectedFilteredBuffers    []*v1.CapacityBuffer
		expectedFilteredOutBuffers []*v1.CapacityBuffer
	}{
		{
			name: "Template generation did not change",
			buffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen(podTempGen3.Name, podTempGen3.Generation),
			},
			expectedFilteredBuffers: []*v1.CapacityBuffer{},
			expectedFilteredOutBuffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen(podTempGen3.Name, podTempGen3.Generation),
			},
		},
		{
			name: "Template generation changed",
			buffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen(podTempGen3.Name, 2),
			},
			expectedFilteredBuffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen(podTempGen3.Name, 2),
			},
			expectedFilteredOutBuffers: []*v1.CapacityBuffer{},
		},
		{
			name: "Template doesn't exist",
			buffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen("randomTemplate", 2),
			},
			expectedFilteredBuffers: []*v1.CapacityBuffer{},
			expectedFilteredOutBuffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen("randomTemplate", 2),
			},
		},
		{
			name: "Multiple templates",
			buffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen(podTempGen4.Name, 3),
				getTestBufferWithPodTempGen(podTempGen3.Name, podTempGen3.Generation),
				getTestBufferWithPodTempGen("randomTemplate", 2),
			},
			expectedFilteredBuffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen(podTempGen4.Name, 3),
			},
			expectedFilteredOutBuffers: []*v1.CapacityBuffer{
				getTestBufferWithPodTempGen(podTempGen3.Name, podTempGen3.Generation),
				getTestBufferWithPodTempGen("randomTemplate", 2),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			generationFilter := NewPodTemplateGenerationChangedFilter(fakeClient)
			filtered, filteredOut := generationFilter.Filter(test.buffers)
			assert.ElementsMatch(t, test.expectedFilteredBuffers, filtered)
			assert.ElementsMatch(t, test.expectedFilteredOutBuffers, filteredOut)
		})
	}
}

func getTestBufferWithPodTempGen(podRefName string, generation int64) *v1.CapacityBuffer {
	return testutil.GetBuffer(nil, nil, nil, &v1.LocalObjectRef{Name: podRefName}, nil, &generation, nil, nil)
}
