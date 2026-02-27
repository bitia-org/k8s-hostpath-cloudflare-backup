package discovery

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestResolveHostPath_CSI(t *testing.T) {
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeAttributes: map[string]string{
						"path": "/data/volumes/pvc-123",
					},
				},
			},
		},
	}
	got := resolveHostPath(pv)
	if got != "/data/volumes/pvc-123" {
		t.Errorf("resolveHostPath(CSI) = %q, want %q", got, "/data/volumes/pvc-123")
	}
}

func TestResolveHostPath_Local(t *testing.T) {
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{
					Path: "/mnt/disks/ssd1",
				},
			},
		},
	}
	got := resolveHostPath(pv)
	if got != "/mnt/disks/ssd1" {
		t.Errorf("resolveHostPath(Local) = %q, want %q", got, "/mnt/disks/ssd1")
	}
}

func TestResolveHostPath_HostPath(t *testing.T) {
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/data",
				},
			},
		},
	}
	got := resolveHostPath(pv)
	if got != "/var/data" {
		t.Errorf("resolveHostPath(HostPath) = %q, want %q", got, "/var/data")
	}
}

func TestResolveHostPath_Empty(t *testing.T) {
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{},
		},
	}
	got := resolveHostPath(pv)
	if got != "" {
		t.Errorf("resolveHostPath(empty) = %q, want empty", got)
	}
}

func TestResolveHostPath_CSIPrecedence(t *testing.T) {
	// CSI should take precedence over HostPath
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeAttributes: map[string]string{
						"path": "/csi-path",
					},
				},
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/hostpath-path",
				},
			},
		},
	}
	got := resolveHostPath(pv)
	if got != "/csi-path" {
		t.Errorf("resolveHostPath(CSI+HostPath) = %q, want %q", got, "/csi-path")
	}
}

func TestPodMountsPVC(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "my-pvc",
						},
					},
				},
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cfg"},
						},
					},
				},
			},
		},
	}

	if !podMountsPVC(pod, "my-pvc") {
		t.Error("podMountsPVC should return true for matching PVC")
	}
	if podMountsPVC(pod, "other-pvc") {
		t.Error("podMountsPVC should return false for non-matching PVC")
	}
}

func TestDiscover_NoPVCs(t *testing.T) {
	client := fake.NewSimpleClientset()
	disc := New(client, false)

	_, err := disc.Discover(context.Background(), "default", "my-release")
	if err == nil {
		t.Fatal("expected error when no PVCs found")
	}
}

func TestDiscover_UnboundPVC(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-pvc",
			Namespace: "default",
			Labels:    map[string]string{"app.kubernetes.io/instance": "my-release"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "", // not bound
		},
	}

	client := fake.NewSimpleClientset(pvc)
	disc := New(client, false)

	_, err := disc.Discover(context.Background(), "default", "my-release")
	if err == nil {
		t.Fatal("expected error for unbound PVC")
	}
}

func TestDiscover_FullChain_StatefulSet(t *testing.T) {
	ns := "test-ns"
	release := "my-app"

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-my-app-0",
			Namespace: ns,
			Labels:    map[string]string{"app.kubernetes.io/instance": release},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-001",
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-001"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/data/pv-001"},
			},
		},
	}

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-app",
			Namespace: ns,
			UID:       "ss-uid-1",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(int32(2)),
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-app-0",
			Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "StatefulSet", Name: "my-app", UID: "ss-uid-1"},
			},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data-my-app-0",
						},
					},
				},
			},
		},
	}

	client := fake.NewSimpleClientset(pvc, pv, ss, pod)
	disc := New(client, false)

	results, err := disc.Discover(context.Background(), ns, release)
	if err != nil {
		t.Fatalf("Discover() error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 PVC, got %d", len(results))
	}

	info := results[0]
	if info.PVCName != "data-my-app-0" {
		t.Errorf("PVCName = %q, want %q", info.PVCName, "data-my-app-0")
	}
	if info.PVName != "pv-001" {
		t.Errorf("PVName = %q, want %q", info.PVName, "pv-001")
	}
	if info.HostPath != "/data/pv-001" {
		t.Errorf("HostPath = %q, want %q", info.HostPath, "/data/pv-001")
	}
	if info.Workload == nil {
		t.Fatal("Workload is nil")
	}
	if info.Workload.Kind != "StatefulSet" {
		t.Errorf("Workload.Kind = %q, want %q", info.Workload.Kind, "StatefulSet")
	}
	if info.Workload.Name != "my-app" {
		t.Errorf("Workload.Name = %q, want %q", info.Workload.Name, "my-app")
	}
	if info.Workload.OriginalReplicas != 2 {
		t.Errorf("Workload.OriginalReplicas = %d, want %d", info.Workload.OriginalReplicas, 2)
	}
}

func TestDiscover_FullChain_Deployment(t *testing.T) {
	ns := "default"
	release := "web"

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-data",
			Namespace: ns,
			Labels:    map[string]string{"app.kubernetes.io/instance": release},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-web",
		},
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-web"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/mnt/data/web"},
			},
		},
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy",
			Namespace: ns,
			UID:       "dep-uid-1",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(3)),
		},
	}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy-abc123",
			Namespace: ns,
			UID:       "rs-uid-1",
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Deployment", Name: "web-deploy", UID: "dep-uid-1"},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy-abc123-xyz",
			Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "ReplicaSet", Name: "web-deploy-abc123", UID: "rs-uid-1"},
			},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "web-data",
						},
					},
				},
			},
		},
	}

	client := fake.NewSimpleClientset(pvc, pv, dep, rs, pod)
	disc := New(client, false)

	results, err := disc.Discover(context.Background(), ns, release)
	if err != nil {
		t.Fatalf("Discover() error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 PVC, got %d", len(results))
	}

	info := results[0]
	if info.Workload == nil {
		t.Fatal("Workload is nil")
	}
	if info.Workload.Kind != "Deployment" {
		t.Errorf("Workload.Kind = %q, want %q", info.Workload.Kind, "Deployment")
	}
	if info.Workload.Name != "web-deploy" {
		t.Errorf("Workload.Name = %q, want %q", info.Workload.Name, "web-deploy")
	}
	if info.Workload.OriginalReplicas != 3 {
		t.Errorf("Workload.OriginalReplicas = %d, want %d", info.Workload.OriginalReplicas, 3)
	}
}
