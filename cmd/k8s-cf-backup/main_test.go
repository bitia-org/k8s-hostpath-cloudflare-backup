package main

import (
	"testing"

	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/types"
)

func TestUniqueWorkloads(t *testing.T) {
	w1 := &types.WorkloadInfo{Kind: "Deployment", Name: "web", Namespace: "default", OriginalReplicas: 2}
	w2 := &types.WorkloadInfo{Kind: "StatefulSet", Name: "db", Namespace: "default", OriginalReplicas: 1}

	pvcs := []types.PVCInfo{
		{PVCName: "pvc-1", Workload: w1},
		{PVCName: "pvc-2", Workload: w1}, // duplicate
		{PVCName: "pvc-3", Workload: w2},
		{PVCName: "pvc-4", Workload: nil}, // no workload
	}

	result := uniqueWorkloads(pvcs)
	if len(result) != 2 {
		t.Fatalf("expected 2 unique workloads, got %d", len(result))
	}
	if result[0].Name != "web" {
		t.Errorf("result[0].Name = %q, want %q", result[0].Name, "web")
	}
	if result[1].Name != "db" {
		t.Errorf("result[1].Name = %q, want %q", result[1].Name, "db")
	}
}

func TestUniqueWorkloads_Empty(t *testing.T) {
	pvcs := []types.PVCInfo{
		{PVCName: "pvc-1", Workload: nil},
	}

	result := uniqueWorkloads(pvcs)
	if len(result) != 0 {
		t.Fatalf("expected 0 workloads, got %d", len(result))
	}
}

func TestUniqueWorkloads_SameNameDifferentKind(t *testing.T) {
	w1 := &types.WorkloadInfo{Kind: "Deployment", Name: "app", Namespace: "default"}
	w2 := &types.WorkloadInfo{Kind: "StatefulSet", Name: "app", Namespace: "default"}

	pvcs := []types.PVCInfo{
		{PVCName: "pvc-1", Workload: w1},
		{PVCName: "pvc-2", Workload: w2},
	}

	result := uniqueWorkloads(pvcs)
	if len(result) != 2 {
		t.Fatalf("expected 2 unique workloads (same name, different kind), got %d", len(result))
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1610612736, "1.5 GB"},
	}

	for _, tc := range tests {
		got := formatSize(tc.input)
		if got != tc.want {
			t.Errorf("formatSize(%d) = %q, want %q", tc.input, got, tc.want)
		}
	}
}
