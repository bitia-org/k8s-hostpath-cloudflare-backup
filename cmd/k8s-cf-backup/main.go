package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/backup"
	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/discovery"
	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/scaler"
	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/types"

	flag "github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const defaultOutputFormat = "{namespace}_{release}_{pvc}_{date}.tar.gz"

func main() {
	var (
		namespace    string
		release      string
		outputFormat string
		outputDir    string
		dryRun       bool
		verbose      bool
		kubeconfig   string
	)

	flag.StringVarP(&namespace, "namespace", "n", "", "Kubernetes namespace (required)")
	flag.StringVarP(&release, "release", "r", "", "Helm release name (required)")
	flag.StringVarP(&outputFormat, "output-format", "o", defaultOutputFormat, "Archive filename template")
	flag.StringVarP(&outputDir, "output-dir", "d", ".", "Output directory for archives")
	flag.BoolVar(&dryRun, "dry-run", false, "Show what would be done without doing it")
	flag.BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig (default: in-cluster or ~/.kube/config)")
	flag.Parse()

	if namespace == "" || release == "" {
		fmt.Fprintln(os.Stderr, "Error: --namespace and --release are required")
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client, err := buildClient(kubeconfig)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	if err := run(ctx, client, namespace, release, outputDir, outputFormat, dryRun, verbose); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func run(ctx context.Context, client kubernetes.Interface, namespace, release, outputDir, outputFormat string, dryRun, verbose bool) error {
	disc := discovery.New(client, verbose)
	sc := scaler.New(client, verbose)
	bk := backup.New(outputDir, outputFormat, verbose)

	// Step 1: Discover PVCs
	fmt.Printf("Discovering PVCs for release %q in namespace %q...\n", release, namespace)
	pvcs, err := disc.Discover(ctx, namespace, release)
	if err != nil {
		return fmt.Errorf("discovery: %w", err)
	}

	fmt.Printf("Found %d PVC(s):\n", len(pvcs))
	for _, pvc := range pvcs {
		workloadStr := "(no workload found)"
		if pvc.Workload != nil {
			workloadStr = fmt.Sprintf("%s/%s (%d replicas)", pvc.Workload.Kind, pvc.Workload.Name, pvc.Workload.OriginalReplicas)
		}
		fmt.Printf("  - %s -> PV %s -> %s [%s]\n", pvc.PVCName, pvc.PVName, pvc.HostPath, workloadStr)
	}

	// Collect unique workloads
	workloads := uniqueWorkloads(pvcs)

	if dryRun {
		printDryRun(pvcs, workloads, outputDir, outputFormat, namespace, release)
		return nil
	}

	// Step 2: Scale down (with deferred scale-back)
	if len(workloads) > 0 {
		fmt.Printf("\nScaling down %d workload(s)...\n", len(workloads))
		// Always scale back, even if backup fails
		defer func() {
			fmt.Println("\nRestoring workload replicas...")
			if err := sc.ScaleBack(ctx, workloads); err != nil {
				log.Printf("WARNING: Failed to restore some workloads: %v", err)
			} else {
				fmt.Println("All workloads restored.")
			}
		}()

		if err := sc.ScaleDown(ctx, workloads); err != nil {
			return fmt.Errorf("scale down: %w", err)
		}
		fmt.Println("All workloads scaled to 0.")
	}

	// Step 3: Backup
	fmt.Printf("\nBacking up %d PVC(s)...\n", len(pvcs))
	results := bk.BackupAll(pvcs, namespace, release)

	// Step 4: Report
	fmt.Println("\n=== Backup Summary ===")
	var hasError bool
	for _, r := range results {
		if r.Err != nil {
			fmt.Printf("  FAIL  %s: %v\n", r.PVCName, r.Err)
			hasError = true
		} else {
			fmt.Printf("  OK    %s -> %s (%s)\n", r.PVCName, r.ArchivePath, formatSize(r.Size))
		}
	}

	if hasError {
		return fmt.Errorf("some backups failed (see above)")
	}
	return nil
}

func uniqueWorkloads(pvcs []types.PVCInfo) []*types.WorkloadInfo {
	seen := make(map[string]bool)
	var result []*types.WorkloadInfo
	for i := range pvcs {
		w := pvcs[i].Workload
		if w == nil {
			continue
		}
		key := w.Kind + "/" + w.Namespace + "/" + w.Name
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, w)
	}
	return result
}

func printDryRun(pvcs []types.PVCInfo, workloads []*types.WorkloadInfo, outputDir, outputFormat, namespace, release string) {
	fmt.Println("\n=== DRY RUN ===")
	if len(workloads) > 0 {
		fmt.Println("\nWould scale down:")
		for _, w := range workloads {
			fmt.Printf("  - %s/%s (currently %d replicas)\n", w.Kind, w.Name, w.OriginalReplicas)
		}
	}
	fmt.Println("\nWould create archives:")
	for _, pvc := range pvcs {
		name := backup.FormatName(outputFormat, namespace, release, pvc.PVCName)
		fmt.Printf("  - %s -> %s\n", pvc.HostPath, filepath.Join(outputDir, name))
	}
	if len(workloads) > 0 {
		fmt.Println("\nWould restore replicas:")
		for _, w := range workloads {
			fmt.Printf("  - %s/%s -> %d replicas\n", w.Kind, w.Name, w.OriginalReplicas)
		}
	}
}

func formatSize(bytes int64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
	)
	switch {
	case bytes >= gb:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func buildClient(kubeconfig string) (kubernetes.Interface, error) {
	var config *rest.Config
	var err error

	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		// Try in-cluster first
		config, err = rest.InClusterConfig()
		if err != nil {
			// Fall back to default kubeconfig
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			configOverrides := &clientcmd.ConfigOverrides{}
			config, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
		}
	}
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}

func init() {
	// Suppress the default log prefix timestamp when not verbose
	// (we use fmt.Printf for user-facing output)
	_ = strings.NewReader("") // avoid unused import if needed
}
