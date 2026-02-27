package scaler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	pollInterval = 2 * time.Second
	waitTimeout  = 5 * time.Minute
)

// Scaler scales workloads down and back up.
type Scaler struct {
	client  kubernetes.Interface
	verbose bool
}

func New(client kubernetes.Interface, verbose bool) *Scaler {
	return &Scaler{client: client, verbose: verbose}
}

// ScaleDown scales all given workloads to 0 replicas and waits for pods to terminate.
func (s *Scaler) ScaleDown(ctx context.Context, workloads []*types.WorkloadInfo) error {
	for _, w := range workloads {
		s.logf("Scaling %s/%s to 0 (was %d)", w.Kind, w.Name, w.OriginalReplicas)
		if err := s.setReplicas(ctx, w, 0); err != nil {
			return fmt.Errorf("scaling down %s/%s: %w", w.Kind, w.Name, err)
		}
	}

	// Wait for all pods to terminate
	for _, w := range workloads {
		if err := s.waitForScale(ctx, w, 0); err != nil {
			return fmt.Errorf("waiting for %s/%s to scale down: %w", w.Kind, w.Name, err)
		}
		s.logf("%s/%s scaled down", w.Kind, w.Name)
	}

	return nil
}

// ScaleBack restores all workloads to their original replica counts.
func (s *Scaler) ScaleBack(ctx context.Context, workloads []*types.WorkloadInfo) error {
	var firstErr error
	for _, w := range workloads {
		s.logf("Restoring %s/%s to %d replicas", w.Kind, w.Name, w.OriginalReplicas)
		if err := s.setReplicas(ctx, w, w.OriginalReplicas); err != nil {
			log.Printf("ERROR: failed to restore %s/%s: %v", w.Kind, w.Name, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (s *Scaler) setReplicas(ctx context.Context, w *types.WorkloadInfo, replicas int32) error {
	switch w.Kind {
	case "Deployment":
		dep, err := s.client.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		dep.Spec.Replicas = &replicas
		_, err = s.client.AppsV1().Deployments(w.Namespace).Update(ctx, dep, metav1.UpdateOptions{})
		return err

	case "StatefulSet":
		ss, err := s.client.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		ss.Spec.Replicas = &replicas
		_, err = s.client.AppsV1().StatefulSets(w.Namespace).Update(ctx, ss, metav1.UpdateOptions{})
		return err

	default:
		return fmt.Errorf("unsupported workload kind: %s", w.Kind)
	}
}

func (s *Scaler) waitForScale(ctx context.Context, w *types.WorkloadInfo, target int32) error {
	deadline := time.After(waitTimeout)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for %s/%s to reach %d replicas", w.Kind, w.Name, target)
		case <-ticker.C:
			ready, err := s.getReadyReplicas(ctx, w)
			if err != nil {
				return err
			}
			s.logf("%s/%s: %d ready replicas (target: %d)", w.Kind, w.Name, ready, target)
			if target == 0 && ready == 0 {
				return nil
			}
			if target > 0 && ready >= target {
				return nil
			}
		}
	}
}

func (s *Scaler) getReadyReplicas(ctx context.Context, w *types.WorkloadInfo) (int32, error) {
	switch w.Kind {
	case "Deployment":
		dep, err := s.client.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		return dep.Status.ReadyReplicas, nil

	case "StatefulSet":
		ss, err := s.client.AppsV1().StatefulSets(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		return ss.Status.ReadyReplicas, nil

	default:
		return 0, fmt.Errorf("unsupported workload kind: %s", w.Kind)
	}
}

func (s *Scaler) logf(format string, args ...interface{}) {
	if s.verbose {
		log.Printf("[scaler] "+format, args...)
	}
}
