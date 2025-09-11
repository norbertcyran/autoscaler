package resourcelimits

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
)

type Provider interface {
	AllLimiters() ([]Limiter, error)
	ResourceLimitersForNode(node *apiv1.Node) ([]Limiter, error)
}
type CloudLimitersProvider struct {
	cloudProvider cloudprovider.CloudProvider
}

func (p *CloudLimitersProvider) AllLimiters() ([]Limiter, error) {
	rl, err := p.cloudProvider.GetResourceLimiter()
	if err != nil {
		return nil, err
	}
	return []Limiter{rl}, nil
}

func (p *CloudLimitersProvider) ResourceLimitersForNode(node *apiv1.Node) ([]Limiter, error) {
	return p.AllLimiters()
}

func NewCloudLimitersProvider(cloudProvider cloudprovider.CloudProvider) *CloudLimitersProvider {
	return &CloudLimitersProvider{
		cloudProvider: cloudProvider,
	}
}
