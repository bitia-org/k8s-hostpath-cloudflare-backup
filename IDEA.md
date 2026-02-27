# k8s-cf-backup

CLI tool that backs up Kubernetes PersistentVolume host paths for a given Helm release.

## What it does

1. Discovers all PVCs belonging to a Helm release (by label `app.kubernetes.io/instance`)
2. Resolves each PVC to its PV and finds the host path on the node
3. Finds workloads (Deployments/StatefulSets) using those PVCs
4. Scales workloads down to 0 to ensure data consistency
5. Creates tar.gz archives of each PV's host path
6. Scales workloads back to original replica counts
7. Reports results

## Usage

```
k8s-cf-backup -n <namespace> -r <release> [-d /backup/dir] [--dry-run] [-v]
```

Runs directly on the node where PV data is stored (same as the bash script it replaces).
