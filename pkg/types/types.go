package types

// PVCInfo holds information about a PersistentVolumeClaim and its backing PV.
type PVCInfo struct {
	Namespace string
	PVCName   string
	PVName    string
	HostPath  string
	Workload  *WorkloadInfo
}

// WorkloadInfo describes a Deployment or StatefulSet that uses a PVC.
type WorkloadInfo struct {
	Kind             string // "Deployment" or "StatefulSet"
	Name             string
	Namespace        string
	OriginalReplicas int32
}

// BackupResult holds the outcome of backing up a single PVC.
type BackupResult struct {
	PVCName     string
	ArchivePath string
	Size        int64
	Err         error
}
