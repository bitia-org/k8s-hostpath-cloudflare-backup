package scaler

import (
	"context"
	"testing"

	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/types"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func TestScaleDown_Deployment(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(3)),
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: 0, // fake client returns this immediately
		},
	}

	client := fake.NewSimpleClientset(dep)
	s := New(client, false)

	workloads := []*types.WorkloadInfo{
		{Kind: "Deployment", Name: "web", Namespace: "default", OriginalReplicas: 3},
	}

	err := s.ScaleDown(context.Background(), workloads)
	if err != nil {
		t.Fatalf("ScaleDown() error: %v", err)
	}

	// Verify replicas were set to 0
	got, err := client.AppsV1().Deployments("default").Get(context.Background(), "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment: %v", err)
	}
	if *got.Spec.Replicas != 0 {
		t.Errorf("replicas = %d, want 0", *got.Spec.Replicas)
	}
}

func TestScaleDown_StatefulSet(t *testing.T) {
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "db",
			Namespace: "prod",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(int32(2)),
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: 0,
		},
	}

	client := fake.NewSimpleClientset(ss)
	s := New(client, false)

	workloads := []*types.WorkloadInfo{
		{Kind: "StatefulSet", Name: "db", Namespace: "prod", OriginalReplicas: 2},
	}

	err := s.ScaleDown(context.Background(), workloads)
	if err != nil {
		t.Fatalf("ScaleDown() error: %v", err)
	}

	got, err := client.AppsV1().StatefulSets("prod").Get(context.Background(), "db", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get statefulset: %v", err)
	}
	if *got.Spec.Replicas != 0 {
		t.Errorf("replicas = %d, want 0", *got.Spec.Replicas)
	}
}

func TestScaleBack_Deployment(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web",
			Namespace: "default",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(0)),
		},
	}

	client := fake.NewSimpleClientset(dep)
	s := New(client, false)

	workloads := []*types.WorkloadInfo{
		{Kind: "Deployment", Name: "web", Namespace: "default", OriginalReplicas: 3},
	}

	err := s.ScaleBack(context.Background(), workloads)
	if err != nil {
		t.Fatalf("ScaleBack() error: %v", err)
	}

	got, err := client.AppsV1().Deployments("default").Get(context.Background(), "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment: %v", err)
	}
	if *got.Spec.Replicas != 3 {
		t.Errorf("replicas = %d, want 3", *got.Spec.Replicas)
	}
}

func TestScaleBack_StatefulSet(t *testing.T) {
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "db",
			Namespace: "prod",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptr.To(int32(0)),
		},
	}

	client := fake.NewSimpleClientset(ss)
	s := New(client, false)

	workloads := []*types.WorkloadInfo{
		{Kind: "StatefulSet", Name: "db", Namespace: "prod", OriginalReplicas: 2},
	}

	err := s.ScaleBack(context.Background(), workloads)
	if err != nil {
		t.Fatalf("ScaleBack() error: %v", err)
	}

	got, err := client.AppsV1().StatefulSets("prod").Get(context.Background(), "db", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get statefulset: %v", err)
	}
	if *got.Spec.Replicas != 2 {
		t.Errorf("replicas = %d, want 2", *got.Spec.Replicas)
	}
}

func TestScaleDown_UnsupportedKind(t *testing.T) {
	client := fake.NewSimpleClientset()
	s := New(client, false)

	workloads := []*types.WorkloadInfo{
		{Kind: "DaemonSet", Name: "agent", Namespace: "kube-system", OriginalReplicas: 1},
	}

	err := s.ScaleDown(context.Background(), workloads)
	if err == nil {
		t.Fatal("expected error for unsupported kind")
	}
}

func TestScaleBack_MultipleWorkloads(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(int32(0))},
	}
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(0))},
	}

	client := fake.NewSimpleClientset(dep, ss)
	s := New(client, false)

	workloads := []*types.WorkloadInfo{
		{Kind: "Deployment", Name: "web", Namespace: "default", OriginalReplicas: 2},
		{Kind: "StatefulSet", Name: "db", Namespace: "default", OriginalReplicas: 1},
	}

	err := s.ScaleBack(context.Background(), workloads)
	if err != nil {
		t.Fatalf("ScaleBack() error: %v", err)
	}

	gotDep, _ := client.AppsV1().Deployments("default").Get(context.Background(), "web", metav1.GetOptions{})
	if *gotDep.Spec.Replicas != 2 {
		t.Errorf("deployment replicas = %d, want 2", *gotDep.Spec.Replicas)
	}

	gotSS, _ := client.AppsV1().StatefulSets("default").Get(context.Background(), "db", metav1.GetOptions{})
	if *gotSS.Spec.Replicas != 1 {
		t.Errorf("statefulset replicas = %d, want 1", *gotSS.Spec.Replicas)
	}
}
