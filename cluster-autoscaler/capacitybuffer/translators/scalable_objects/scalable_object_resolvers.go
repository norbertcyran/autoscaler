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

package scalableobject

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Kinds of the supported objects
const (
	DeploymentKind            = "Deployment"
	ReplicaSetKind            = "ReplicaSet"
	StatefulSetKind           = "StatefulSet"
	ReplicationControllerKind = "ReplicationController"
	JobKind                   = "Job"
	ApiGroupApps              = "apps"
	ApiGroupBatch             = "batch"
	ApiGroupCore              = "core"
)

// ScaleObjectPodResolver resolves scale objects into pod specs and number of replicas only if there is at least one exiting pod
type ScaleObjectPodResolver struct {
	client client.Client
}

// NewScaleObjectPodResolver returns new ScaleObjectPodResolver
func NewScaleObjectPodResolver(client client.Client) *ScaleObjectPodResolver {
	return &ScaleObjectPodResolver{
		client: client,
	}
}

// GetTemplateAndReplicas returns the pod spec template of the passed object name and namespace
func (s *ScaleObjectPodResolver) GetTemplateAndReplicas(namespace, group, kind, name string) (*corev1.PodTemplateSpec, *int32, error) {
	ctx := context.TODO()
	scale, err := s.getScaleSubresource(ctx, namespace, group, kind, name)
	if err != nil {
		return nil, nil, err
	}

	pods, err := s.getPodsForScale(ctx, namespace, scale)
	if err != nil {
		return nil, nil, err
	}

	if len(pods) == 0 {
		return nil, &scale.Status.Replicas, nil
	}
	pod := getMostRecentPod(pods)
	return buildPodTemplateFromPod(pod), &scale.Status.Replicas, nil
}

func (s *ScaleObjectPodResolver) getScaleSubresource(ctx context.Context, namespace, group, kind, name string) (*autoscalingv1.Scale, error) {
	mapping, err := s.client.RESTMapper().RESTMapping(schema.GroupKind{Group: group, Kind: kind})
	if err != nil {
		return nil, fmt.Errorf("failed to get REST mapping: %w", err)
	}

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(mapping.GroupVersionKind)
	u.SetNamespace(namespace)
	u.SetName(name)

	scale := &autoscalingv1.Scale{}
	err = s.client.SubResource("scale").Get(ctx, u, scale)
	if err != nil {
		return nil, fmt.Errorf("failed to get scale: %w", err)
	}
	return scale, nil
}

func (s *ScaleObjectPodResolver) getPodsForScale(ctx context.Context, namespace string, scale *autoscalingv1.Scale) ([]corev1.Pod, error) {
	selector, err := labels.Parse(scale.Status.Selector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse selector: %w", err)
	}

	podsList := &corev1.PodList{}
	err = s.client.List(ctx, podsList, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: selector})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}
	return podsList.Items, nil
}

func getMostRecentPod(podList []corev1.Pod) *corev1.Pod {
	sort.Slice(podList, func(i, j int) bool {
		return podList[i].CreationTimestamp.After(podList[j].CreationTimestamp.Time)
	})
	return &podList[0]
}

func buildPodTemplateFromPod(pod *corev1.Pod) *corev1.PodTemplateSpec {
	pod.Spec.NodeName = ""
	return &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      pod.Labels,
			Annotations: pod.Annotations,
		},
		Spec: pod.Spec,
	}
}

// GetSupportedScalableObjectResolvers returns the default ScalableObjectResolvers
func GetSupportedScalableObjectResolvers(client client.Client) []ScalableObjectTemplateResolver {
	return []ScalableObjectTemplateResolver{
		&deployment{scalableObjectTemplateResolver{client: client, kind: DeploymentKind, apiGroup: ApiGroupApps}},
		&replicaSet{scalableObjectTemplateResolver{client: client, kind: ReplicaSetKind, apiGroup: ApiGroupApps}},
		&statefulSet{scalableObjectTemplateResolver{client: client, kind: StatefulSetKind, apiGroup: ApiGroupApps}},
		&replicationController{scalableObjectTemplateResolver{client: client, kind: ReplicationControllerKind, apiGroup: ApiGroupCore}},
		&job{scalableObjectTemplateResolver{client: client, kind: JobKind, apiGroup: ApiGroupBatch}},
	}
}

// ScalableObjectTemplateResolver is an interface for resolvers that are supported to get pod spec template for a any object
type ScalableObjectTemplateResolver interface {
	GetTemplateAndReplicas(namespace, name string) (*corev1.PodTemplateSpec, *int32, error)
	GetResolverKey() string
}

type scalableObjectTemplateResolver struct {
	client   client.Client
	kind     string
	apiGroup string
}

// GetResolverKey returns a string that distinguishes the resolver by api group and king
func (s *scalableObjectTemplateResolver) GetResolverKey() string {
	return GetResolverKey(s.apiGroup, s.kind)
}

// GetResolverKey returns the key of a resolver given the api group and kind
func GetResolverKey(apiGroup, kind string) string {
	return fmt.Sprintf("%v-%v", apiGroup, kind)
}

type deployment struct{ scalableObjectTemplateResolver }

// GetTemplateAndReplicas returns the pod spec template of the passed object name and namespace
func (s *deployment) GetTemplateAndReplicas(namespace, name string) (*corev1.PodTemplateSpec, *int32, error) {
	obj := &appsv1.Deployment{}
	err := s.client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get Deployment: %w", err)
	}
	return obj.Spec.Template.DeepCopy(), obj.Spec.Replicas, nil
}

type replicaSet struct{ scalableObjectTemplateResolver }

// GetTemplateAndReplicas returns the pod spec template of the passed object name and namespace
func (s *replicaSet) GetTemplateAndReplicas(namespace, name string) (*corev1.PodTemplateSpec, *int32, error) {
	obj := &appsv1.ReplicaSet{}
	err := s.client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get ReplicaSet: %w", err)
	}
	return obj.Spec.Template.DeepCopy(), obj.Spec.Replicas, nil
}

type statefulSet struct{ scalableObjectTemplateResolver }

// GetTemplateAndReplicas returns the pod spec template of the passed object name and namespace
func (s *statefulSet) GetTemplateAndReplicas(namespace, name string) (*corev1.PodTemplateSpec, *int32, error) {
	obj := &appsv1.StatefulSet{}
	err := s.client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get StatefulSet: %w", err)
	}
	return obj.Spec.Template.DeepCopy(), obj.Spec.Replicas, nil
}

type replicationController struct{ scalableObjectTemplateResolver }

// GetTemplateAndReplicas returns the pod spec template of the passed object name and namespace
func (s *replicationController) GetTemplateAndReplicas(namespace, name string) (*corev1.PodTemplateSpec, *int32, error) {
	obj := &corev1.ReplicationController{}
	err := s.client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get ReplicationController: %w", err)
	}
	return obj.Spec.Template.DeepCopy(), obj.Spec.Replicas, nil
}

type job struct{ scalableObjectTemplateResolver }

// GetTemplateAndReplicas returns the pod spec template of the passed object name and namespace
func (s *job) GetTemplateAndReplicas(namespace, name string) (*corev1.PodTemplateSpec, *int32, error) {
	obj := &batchv1.Job{}
	err := s.client.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get Job: %w", err)
	}
	return obj.Spec.Template.DeepCopy(), obj.Spec.Parallelism, nil
}
