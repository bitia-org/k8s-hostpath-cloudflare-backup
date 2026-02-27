package discovery

import (
	"context"
	"fmt"
	"log"

	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/types"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Discoverer finds PVCs, resolves PVs, and identifies owning workloads for a Helm release.
type Discoverer struct {
	client  kubernetes.Interface
	verbose bool
}

func New(client kubernetes.Interface, verbose bool) *Discoverer {
	return &Discoverer{client: client, verbose: verbose}
}

// Discover finds all PVCs for the given Helm release and resolves their PV host paths
// and owning workloads.
func (d *Discoverer) Discover(ctx context.Context, namespace, release string) ([]types.PVCInfo, error) {
	pvcs, err := d.findPVCs(ctx, namespace, release)
	if err != nil {
		return nil, fmt.Errorf("finding PVCs: %w", err)
	}

	if len(pvcs) == 0 {
		return nil, fmt.Errorf("no PVCs found for release %q in namespace %q", release, namespace)
	}

	var results []types.PVCInfo
	for _, pvc := range pvcs {
		info, err := d.resolvePVC(ctx, &pvc)
		if err != nil {
			return nil, fmt.Errorf("resolving PVC %q: %w", pvc.Name, err)
		}
		results = append(results, *info)
	}

	return results, nil
}

func (d *Discoverer) findPVCs(ctx context.Context, namespace, release string) ([]corev1.PersistentVolumeClaim, error) {
	labelSelector := fmt.Sprintf("app.kubernetes.io/instance=%s", release)
	d.logf("Listing PVCs in %s with selector %q", namespace, labelSelector)

	pvcList, err := d.client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, err
	}

	d.logf("Found %d PVCs", len(pvcList.Items))
	return pvcList.Items, nil
}

func (d *Discoverer) resolvePVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (*types.PVCInfo, error) {
	info := &types.PVCInfo{
		Namespace: pvc.Namespace,
		PVCName:   pvc.Name,
	}

	// Resolve PV
	if pvc.Spec.VolumeName == "" {
		return nil, fmt.Errorf("PVC %q is not bound to a PV", pvc.Name)
	}
	info.PVName = pvc.Spec.VolumeName

	pv, err := d.client.CoreV1().PersistentVolumes().Get(ctx, info.PVName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting PV %q: %w", info.PVName, err)
	}

	info.HostPath = resolveHostPath(pv)
	if info.HostPath == "" {
		return nil, fmt.Errorf("could not resolve host path for PV %q", info.PVName)
	}
	d.logf("PVC %s -> PV %s -> path %s", info.PVCName, info.PVName, info.HostPath)

	// Find owning workload
	workload, err := d.findWorkload(ctx, pvc)
	if err != nil {
		d.logf("Warning: could not find workload for PVC %q: %v", pvc.Name, err)
	}
	info.Workload = workload

	return info, nil
}

// resolveHostPath extracts the host path from a PV spec.
// Supports CSI volumeAttributes, local volumes, and hostPath volumes.
func resolveHostPath(pv *corev1.PersistentVolume) string {
	// CSI with volumeAttributes.path (e.g. hostpath provisioner)
	if pv.Spec.CSI != nil {
		if path, ok := pv.Spec.CSI.VolumeAttributes["path"]; ok {
			return path
		}
	}

	// Local volume
	if pv.Spec.Local != nil {
		return pv.Spec.Local.Path
	}

	// HostPath volume
	if pv.Spec.HostPath != nil {
		return pv.Spec.HostPath.Path
	}

	return ""
}

// findWorkload finds the Deployment or StatefulSet that owns pods mounting the given PVC.
func (d *Discoverer) findWorkload(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (*types.WorkloadInfo, error) {
	// List pods in the namespace
	pods, err := d.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing pods: %w", err)
	}

	// Find pods that mount this PVC
	for _, pod := range pods.Items {
		if !podMountsPVC(&pod, pvc.Name) {
			continue
		}
		d.logf("Pod %s mounts PVC %s", pod.Name, pvc.Name)

		// Walk owner references to find Deployment or StatefulSet
		workload, err := d.resolveOwner(ctx, &pod)
		if err != nil {
			d.logf("Warning: could not resolve owner for pod %q: %v", pod.Name, err)
			continue
		}
		if workload != nil {
			d.logf("PVC %s owned by %s/%s", pvc.Name, workload.Kind, workload.Name)
			return workload, nil
		}
	}

	return nil, fmt.Errorf("no workload found mounting PVC %q", pvc.Name)
}

func podMountsPVC(pod *corev1.Pod, pvcName string) bool {
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvcName {
			return true
		}
	}
	return false
}

// resolveOwner walks the owner reference chain from a pod to find a Deployment or StatefulSet.
func (d *Discoverer) resolveOwner(ctx context.Context, pod *corev1.Pod) (*types.WorkloadInfo, error) {
	ns := pod.Namespace

	for _, ref := range pod.OwnerReferences {
		switch ref.Kind {
		case "StatefulSet":
			ss, err := d.client.AppsV1().StatefulSets(ns).Get(ctx, ref.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			return statefulSetInfo(ss), nil

		case "ReplicaSet":
			rs, err := d.client.AppsV1().ReplicaSets(ns).Get(ctx, ref.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			// ReplicaSet is owned by a Deployment
			for _, rsRef := range rs.OwnerReferences {
				if rsRef.Kind == "Deployment" {
					dep, err := d.client.AppsV1().Deployments(ns).Get(ctx, rsRef.Name, metav1.GetOptions{})
					if err != nil {
						return nil, err
					}
					return deploymentInfo(dep), nil
				}
			}
		}
	}

	return nil, nil
}

func deploymentInfo(dep *appsv1.Deployment) *types.WorkloadInfo {
	var replicas int32 = 1
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}
	return &types.WorkloadInfo{
		Kind:             "Deployment",
		Name:             dep.Name,
		Namespace:        dep.Namespace,
		OriginalReplicas: replicas,
	}
}

func statefulSetInfo(ss *appsv1.StatefulSet) *types.WorkloadInfo {
	var replicas int32 = 1
	if ss.Spec.Replicas != nil {
		replicas = *ss.Spec.Replicas
	}
	return &types.WorkloadInfo{
		Kind:             "StatefulSet",
		Name:             ss.Name,
		Namespace:        ss.Namespace,
		OriginalReplicas: replicas,
	}
}

func (d *Discoverer) logf(format string, args ...interface{}) {
	if d.verbose {
		log.Printf("[discovery] "+format, args...)
	}
}
