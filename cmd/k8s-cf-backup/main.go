package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
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

type restoreTask struct {
	archivePath string
	pvc         types.PVCInfo
}

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

	// Subcommand routing: first positional arg is "backup" or "restore"
	args := flag.Args()
	subcommand := "backup"
	if len(args) > 0 && (args[0] == "backup" || args[0] == "restore") {
		subcommand = args[0]
		args = args[1:]
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client, err := buildClient(kubeconfig)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	switch subcommand {
	case "backup":
		if err := run(ctx, client, namespace, release, outputDir, outputFormat, dryRun, verbose); err != nil {
			log.Fatalf("Error: %v", err)
		}
	case "restore":
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "Error: restore requires at least one archive file")
			flag.Usage()
			os.Exit(1)
		}
		if err := runRestore(ctx, client, namespace, release, outputFormat, args, dryRun, verbose); err != nil {
			log.Fatalf("Error: %v", err)
		}
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

func runRestore(ctx context.Context, client kubernetes.Interface, namespace, release, outputFormat string, archives []string, dryRun, verbose bool) error {
	disc := discovery.New(client, verbose)
	sc := scaler.New(client, verbose)
	bk := backup.New("", "", verbose)

	// Step 1: Parse archive filenames to extract PVC names
	type archiveMapping struct {
		path    string
		pvcName string
	}
	var mappings []archiveMapping
	for _, archive := range archives {
		pvcName, err := parseArchiveName(archive, outputFormat, namespace, release)
		if err != nil {
			return fmt.Errorf("parsing archive %q: %w", archive, err)
		}
		mappings = append(mappings, archiveMapping{path: archive, pvcName: pvcName})
	}

	fmt.Printf("Parsed %d archive(s):\n", len(mappings))
	for _, m := range mappings {
		fmt.Printf("  - %s -> PVC %s\n", filepath.Base(m.path), m.pvcName)
	}

	// Step 2: Discover PVCs for the release
	fmt.Printf("\nDiscovering PVCs for release %q in namespace %q...\n", release, namespace)
	pvcs, err := disc.Discover(ctx, namespace, release)
	if err != nil {
		return fmt.Errorf("discovery: %w", err)
	}

	// Step 3: Match archives to discovered PVCs
	pvcMap := make(map[string]types.PVCInfo)
	for _, pvc := range pvcs {
		pvcMap[pvc.PVCName] = pvc
	}

	var tasks []restoreTask
	for _, m := range mappings {
		pvc, ok := pvcMap[m.pvcName]
		if !ok {
			return fmt.Errorf("PVC %q (from archive %q) not found in release %q", m.pvcName, filepath.Base(m.path), release)
		}
		tasks = append(tasks, restoreTask{archivePath: m.path, pvc: pvc})
	}

	fmt.Printf("Matched %d archive(s) to PVC(s):\n", len(tasks))
	for _, t := range tasks {
		fmt.Printf("  - %s -> %s (host path: %s)\n", filepath.Base(t.archivePath), t.pvc.PVCName, t.pvc.HostPath)
	}

	// Collect workloads from matched PVCs
	var matchedPVCs []types.PVCInfo
	for _, t := range tasks {
		matchedPVCs = append(matchedPVCs, t.pvc)
	}
	workloads := uniqueWorkloads(matchedPVCs)

	if dryRun {
		printRestoreDryRun(tasks, workloads)
		return nil
	}

	// Step 4: Scale down
	if len(workloads) > 0 {
		fmt.Printf("\nScaling down %d workload(s)...\n", len(workloads))
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

	// Step 5: Restore each archive
	fmt.Printf("\nRestoring %d PVC(s)...\n", len(tasks))
	var hasError bool
	for _, t := range tasks {
		fmt.Printf("  Restoring %s -> %s\n", filepath.Base(t.archivePath), t.pvc.HostPath)
		if err := bk.RestoreOne(t.archivePath, t.pvc.HostPath); err != nil {
			fmt.Printf("  FAIL  %s: %v\n", t.pvc.PVCName, err)
			hasError = true
		} else {
			fmt.Printf("  OK    %s\n", t.pvc.PVCName)
		}
	}

	// Step 6: Scale back (deferred above)

	// Step 7: Report
	fmt.Println("\n=== Restore Summary ===")
	for _, t := range tasks {
		fmt.Printf("  %s -> %s\n", filepath.Base(t.archivePath), t.pvc.PVCName)
	}

	if hasError {
		return fmt.Errorf("some restores failed (see above)")
	}
	return nil
}

// parseArchiveName extracts the PVC name from an archive filename using the output format pattern.
// It replaces {namespace} and {release} with their known values, {date} with a wildcard,
// and captures {pvc} via a regex group.
func parseArchiveName(archivePath, format, namespace, release string) (string, error) {
	filename := filepath.Base(archivePath)

	// Escape the format as a regex literal, then replace placeholders
	pattern := regexp.QuoteMeta(format)
	pattern = strings.ReplaceAll(pattern, regexp.QuoteMeta("{namespace}"), regexp.QuoteMeta(namespace))
	pattern = strings.ReplaceAll(pattern, regexp.QuoteMeta("{release}"), regexp.QuoteMeta(release))
	pattern = strings.ReplaceAll(pattern, regexp.QuoteMeta("{pvc}"), "(.+?)")
	pattern = strings.ReplaceAll(pattern, regexp.QuoteMeta("{date}"), ".+")
	pattern = "^" + pattern + "$"

	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", fmt.Errorf("invalid format pattern: %w", err)
	}

	matches := re.FindStringSubmatch(filename)
	if matches == nil {
		return "", fmt.Errorf("filename %q does not match format %q", filename, format)
	}

	return matches[1], nil
}

func printRestoreDryRun(tasks []restoreTask, workloads []*types.WorkloadInfo) {
	fmt.Println("\n=== DRY RUN ===")
	if len(workloads) > 0 {
		fmt.Println("\nWould scale down:")
		for _, w := range workloads {
			fmt.Printf("  - %s/%s (currently %d replicas)\n", w.Kind, w.Name, w.OriginalReplicas)
		}
	}
	fmt.Println("\nWould restore:")
	for _, t := range tasks {
		fmt.Printf("  - %s -> %s (host path: %s)\n", filepath.Base(t.archivePath), t.pvc.PVCName, t.pvc.HostPath)
	}
	if len(workloads) > 0 {
		fmt.Println("\nWould restore replicas:")
		for _, w := range workloads {
			fmt.Printf("  - %s/%s -> %d replicas\n", w.Kind, w.Name, w.OriginalReplicas)
		}
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
