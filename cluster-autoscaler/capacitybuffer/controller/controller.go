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

package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	v1 "k8s.io/autoscaler/cluster-autoscaler/apis/capacitybuffer/autoscaling.x-k8s.io/v1beta1"
	"k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/fakepods"
	filters "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/filters"
	cbmetrics "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/metrics"
	translators "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/translators"
	updater "k8s.io/autoscaler/cluster-autoscaler/capacitybuffer/updater"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	podTemplateRefIndex      = "podTemplateRef"
	fullReconciliationPeriod = 5 * time.Minute
)

// CapacityBufferReconciler performs updates on Buffers and convert them to pods to be injected
type CapacityBufferReconciler struct {
	client                  client.Client
	strategyFilter          filters.Filter
	translator              translators.Translator
	quotaAllocator          *resourceQuotaAllocator
	updater                 updater.StatusUpdater
	clock                   clock.Clock
	reconciliationTimeCache *cbmetrics.ReconciliationCache
}

// NewCapacityBufferReconciler creates a new CapacityBufferReconciler
func NewCapacityBufferReconciler(
	client client.Client,
	resolver fakepods.Resolver,
	strategies []string,
	reconciliationTimeCache *cbmetrics.ReconciliationCache,
	clock clock.Clock,
) *CapacityBufferReconciler {
	return &CapacityBufferReconciler{
		client:         client,
		strategyFilter: filters.NewStrategyFilter(strategies),
		translator: translators.NewCombinedTranslator(
			[]translators.Translator{
				translators.NewPodTemplateBufferTranslator(client, resolver),
				translators.NewDefaultScalableObjectsTranslator(client, resolver),
			},
		),
		quotaAllocator:          newResourceQuotaAllocator(client),
		updater:                 *updater.NewStatusUpdater(client),
		clock:                   clock,
		reconciliationTimeCache: reconciliationTimeCache,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *CapacityBufferReconciler) SetupWithManager(ctx context.Context, mgr manager.Manager) error {
	// Register index for PodTemplateRef
	err := mgr.GetCache().IndexField(ctx, &v1.CapacityBuffer{}, podTemplateRefIndex, func(obj client.Object) []string {
		buffer, ok := obj.(*v1.CapacityBuffer)
		if !ok {
			return nil
		}
		if buffer.Spec.PodTemplateRef != nil {
			return []string{buffer.Spec.PodTemplateRef.Name}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to add indexers: %w", err)
	}

	// Reconcile buffers on ResourceQuota status changes
	rqPredicate := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldQuota := e.ObjectOld.(*corev1.ResourceQuota)
			newQuota := e.ObjectNew.(*corev1.ResourceQuota)

			// Reconcile only on Status changes (Status.Hard and Status.Used)
			if equality.Semantic.DeepEqual(oldQuota.Status.Hard, newQuota.Status.Hard) &&
				equality.Semantic.DeepEqual(oldQuota.Status.Used, newQuota.Status.Used) {
				return false
			}
			return true
		},
	}

	ch := newFullReconciliationTrigger(ctx, fullReconciliationPeriod)

	return ctrl.NewControllerManagedBy(mgr).
		Named("capacitybuffer").
		Watches(&v1.CapacityBuffer{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: obj.GetNamespace()}}}
		}), builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.ResourceQuota{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: obj.GetNamespace()}}}
		}), builder.WithPredicates(rqPredicate)).
		Watches(&corev1.PodTemplate{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			template := obj.(*corev1.PodTemplate)

			var buffers v1.CapacityBufferList
			err := r.client.List(ctx, &buffers, client.InNamespace(template.Namespace), client.MatchingFields{podTemplateRefIndex: template.Name})
			if err != nil {
				runtime.HandleError(fmt.Errorf("error looking up buffers for pod template %s: %w", template.Name, err))
				return nil
			}

			var requests []reconcile.Request
			if len(buffers.Items) > 0 {
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: template.Namespace}})
			}
			return requests
		}), builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		WatchesRawSource(source.Channel(ch, handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, _ struct{}) []reconcile.Request {
			var buffers v1.CapacityBufferList
			if err := mgr.GetClient().List(ctx, &buffers); err != nil {
				return nil
			}

			namespaces := make(map[string]bool)
			for _, b := range buffers.Items {
				namespaces[b.Namespace] = true
			}

			var requests []reconcile.Request
			for ns := range namespaces {
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: ns}})
			}
			return requests
		}))).
		Complete(r)
}

// newFullReconciliationTrigger returns a channel that sends an event every fullReconciliationPeriod
func newFullReconciliationTrigger(ctx context.Context, period time.Duration) <-chan event.TypedGenericEvent[struct{}] {
	ch := make(chan event.TypedGenericEvent[struct{}])
	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Send a dummy event to trigger the reconciliation
				ch <- event.TypedGenericEvent[struct{}]{Object: struct{}{}}
			}
		}
	}()
	return ch
}

// Reconcile reconciles all buffers in a namespace.
//
// We must reconcile all buffers in a namespace because of resource quota allocation.
// If one buffer in a namespace changes, e.g. it requests more resources,
// it may impact other buffers in the namespace.
func (r *CapacityBufferReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	namespace := req.Name // We stored namespace in Name
	klog.V(5).Infof("CapacityBuffer controller: reconciling namespace: %s", namespace)

	// List all capacity buffers in the target namespace
	var buffers v1.CapacityBufferList
	err := r.client.List(ctx, &buffers, client.InNamespace(namespace))
	if err != nil {
		return reconcile.Result{}, err
	}

	buffersPtrs := make([]*v1.CapacityBuffer, len(buffers.Items))
	for i := range buffers.Items {
		buffersPtrs[i] = &buffers.Items[i]
	}

	// Filter the desired provisioning strategy
	filteredBuffers, filteredOutBuffers := r.strategyFilter.Filter(buffersPtrs)

	// Update reconciliation time for filtered out buffers
	r.updateReconciliationTimeCache(filteredOutBuffers)

	if len(filteredBuffers) == 0 {
		return reconcile.Result{}, nil
	}

	// Sort buffers deterministically by CreationTimestamp, then Name. Stable order
	// is required to prevent flakiness of resource quotas allocation.
	sort.Slice(filteredBuffers, func(i, j int) bool {
		if filteredBuffers[i].CreationTimestamp.Time.Equal(filteredBuffers[j].CreationTimestamp.Time) {
			return filteredBuffers[i].Name < filteredBuffers[j].Name
		}
		return filteredBuffers[i].CreationTimestamp.Before(&filteredBuffers[j].CreationTimestamp)
	})

	// Extract pod specs and number of replicas from filtered buffers
	translationErrors := r.translator.Translate(filteredBuffers)
	for _, err := range translationErrors {
		runtime.HandleError(fmt.Errorf("capacity buffer controller error: %w", err))
	}

	// Allocate resource quotas
	allocationErrors := r.quotaAllocator.Allocate(ctx, namespace, filteredBuffers)
	for _, err := range allocationErrors {
		runtime.HandleError(fmt.Errorf("capacity buffer controller error: %w", err))
	}

	// Update buffer status by calling API server
	updatedBuffers, updateErrors := r.updater.Update(filteredBuffers)
	r.updateReconciliationTimeCache(updatedBuffers)
	for _, err := range updateErrors {
		runtime.HandleError(fmt.Errorf("capacity buffer controller error: %w", err))
	}

	// If there were any errors, return one to trigger requeue
	if len(translationErrors) > 0 || len(allocationErrors) > 0 || len(updateErrors) > 0 {
		return reconcile.Result{}, errors.New("encountered errors during reconciliation")
	}

	return reconcile.Result{}, nil
}

func (r *CapacityBufferReconciler) updateReconciliationTimeCache(buffers []*v1.CapacityBuffer) {
	if r.reconciliationTimeCache == nil || len(buffers) == 0 {
		return
	}
	r.reconciliationTimeCache.Update(buffers, r.clock.Now())
}
