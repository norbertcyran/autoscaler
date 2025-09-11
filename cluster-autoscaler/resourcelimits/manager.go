package resourcelimits

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/utils"
	"k8s.io/autoscaler/cluster-autoscaler/processors/customresources"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

type Manager struct {
	crp            customresources.CustomResourcesProcessor
	limitProviders []Provider
	usages         map[string]map[string]int64
}

//type LimitProvider interface {
//	LimitsForNode(nodeInfo *framework.NodeInfo) (*LimitsUsage, error)
//}

// Delta is a map: the key is resource type and the value is resource delta.
type Delta map[string]int64

type Limiter interface {
	ID() string
	MatchesNode(node *corev1.Node) bool
	MaxLimits() map[string]int64
	MinLimits() map[string]int64
}

func (m *Manager) RecalculateUsages(ctx *context.AutoscalingContext, nodes []*corev1.Node) (err errors.AutoscalerError) {
	m.usages = make(map[string]map[string]int64)
	var limiters []Limiter
	for _, provider := range m.limitProviders {
		provLimiters, err := provider.AllLimiters()
		if err != nil {
			return errors.ToAutoscalerError(errors.CloudProviderError, err).AddPrefix("failed to get limiters from provider")
		}
		limiters = append(limiters, provLimiters...)
	}
	for _, node := range nodes {
		cores, memory := utils.GetNodeCoresAndMemory(node)
		resourceTargets, err := m.crp.GetNodeResourceTargets(ctx, node, nil)
		if err != nil {
			return errors.ToAutoscalerError(errors.CloudProviderError, err).AddPrefix("failed to get custom resource target for node %v: ", node.Name)
		}
		for _, rl := range limiters {
			if rl.MatchesNode(node) {
				if m.usages[rl.ID()] == nil {
					m.usages[rl.ID()] = make(map[string]int64)
				}
				m.usages[rl.ID()][cloudprovider.ResourceNameCores] += cores
				m.usages[rl.ID()][cloudprovider.ResourceNameMemory] += memory
				for _, resourceTarget := range resourceTargets {
					if resourceTarget.ResourceType == "" || resourceTarget.ResourceCount == 0 {
						continue
					}
					m.usages[rl.ID()][resourceTarget.ResourceType] += resourceTarget.ResourceCount
				}
			}
		}
	}
	return nil
}

// DeltaForNode calculates the amount of resources that will be used from the cluster when creating a node.
func (m *Manager) DeltaForNode(ctx *context.AutoscalingContext, node *corev1.Node, nodeGroup cloudprovider.NodeGroup) (Delta, errors.AutoscalerError) {
	resultScaleUpDelta := make(Delta)
	nodeCPU, nodeMemory := utils.GetNodeCoresAndMemory(node)
	resultScaleUpDelta[cloudprovider.ResourceNameCores] = nodeCPU
	resultScaleUpDelta[cloudprovider.ResourceNameMemory] = nodeMemory

	resourceTargets, err := m.crp.GetNodeResourceTargets(ctx, node, nodeGroup)
	if err != nil {
		return Delta{}, errors.ToAutoscalerError(errors.CloudProviderError, err).AddPrefix("failed to get target custom resources for node group %v: ", nodeGroup.Id())
	}

	for _, resourceTarget := range resourceTargets {
		resultScaleUpDelta[resourceTarget.ResourceType] = resourceTarget.ResourceCount
	}

	return resultScaleUpDelta, nil
}

func (m *Manager) ApplyNodeDelta(ctx *context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeDelta int) (*ApplyDeltaResult, error) {
	nodeInfo, err := nodeGroup.TemplateNodeInfo()
	if err != nil {
		return nil, err
	}
	node := nodeInfo.Node()
	delta, err := m.DeltaForNode(ctx, node, nodeGroup)
	if err != nil {
		return nil, err
	}
	matchingLimiters, err := m.matchingLimiters(node)
	if err != nil {
		return nil, err
	}

	result := m.checkNodeDelta(delta, matchingLimiters, nodeDelta)

	if result.AllowedDelta != nodeDelta {
		return result, nil
	}

	for _, rl := range matchingLimiters {
		if m.usages[rl.ID()] == nil {
			m.usages[rl.ID()] = make(map[string]int64)
		}
		for resource, resourceDelta := range delta {
			m.usages[rl.ID()][resource] += resourceDelta * int64(nodeDelta)
		}
	}

	return result, nil

}

func (m *Manager) checkNodeDelta(delta Delta, matchingLimiters []Limiter, nodeDelta int) *ApplyDeltaResult {
	result := &ApplyDeltaResult{
		AllowedDelta: nodeDelta,
	}

	exceededResources := make(sets.Set[string])
	for _, rl := range matchingLimiters {
		limiterUsages := m.usages[rl.ID()]
		for resource, resourceDelta := range delta {
			max := rl.MaxLimits()[resource]
			min := rl.MinLimits()[resource]

			var currentUsage int64
			if limiterUsages != nil {
				currentUsage = limiterUsages[resource]
			}
			newUsage := currentUsage + resourceDelta*int64(nodeDelta)

			if nodeDelta > 0 { // Adding nodes
				if max > 0 && newUsage > max {
					if resourceDelta > 0 {
						allowedNodes := (max - currentUsage) / resourceDelta
						if allowedNodes < int64(result.AllowedDelta) {
							result.AllowedDelta = int(allowedNodes)
						}
					}
					exceededResources.Insert(resource)
				}
			} else if nodeDelta < 0 { // Removing nodes
				if min > 0 && newUsage < min {
					if resourceDelta > 0 {
						allowedNodes := (min - currentUsage) / resourceDelta
						if allowedNodes > int64(result.AllowedDelta) {
							result.AllowedDelta = int(allowedNodes)
						}
					}
					exceededResources.Insert(resource)
				}
			}
		}
	}
	result.ExceededResources = exceededResources.UnsortedList()
	return result
}

func (m *Manager) CheckNodeDelta(ctx *context.AutoscalingContext, nodeGroup cloudprovider.NodeGroup, nodeDelta int) (*ApplyDeltaResult, error) {
	nodeInfo, err := nodeGroup.TemplateNodeInfo()
	if err != nil {
		return nil, err
	}
	node := nodeInfo.Node()
	delta, err := m.DeltaForNode(ctx, node, nodeGroup)
	if err != nil {
		return nil, err
	}
	matchingLimiters, err := m.matchingLimiters(node)
	if err != nil {
		return nil, err
	}
	return m.checkNodeDelta(delta, matchingLimiters, nodeDelta), nil
}

func (m *Manager) matchingLimiters(node *corev1.Node) ([]Limiter, error) {
	var matchingLimiters []Limiter
	for _, provider := range m.limitProviders {
		limiters, err := provider.ResourceLimitersForNode(node)
		if err != nil {
			return nil, err
		}
		matchingLimiters = append(matchingLimiters, limiters...)
	}
	return matchingLimiters, nil
}

type ResourceManagerFactory struct {
	crp            customresources.CustomResourcesProcessor
	limitProviders []Provider
}

func NewResourceManager(crp customresources.CustomResourcesProcessor, limitProviders []Provider) *Manager {
	return &Manager{
		crp:            crp,
		limitProviders: limitProviders,
		usages:         make(map[string]map[string]int64),
	}
}

type usageTracker struct {
	usages map[string]map[string]int64
}

func newUsageTracker() *usageTracker {
	return &usageTracker{
		usages: make(map[string]map[string]int64),
	}
}

type ApplyDeltaResult struct {
	ExceededResources []string
	AllowedDelta      int
}

func (r *ApplyDeltaResult) Exceeded() bool {
	return len(r.ExceededResources) > 0
}
