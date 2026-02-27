package backup

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/types"
)

func TestFormatName(t *testing.T) {
	b := &Backuper{outputFormat: "{namespace}_{release}_{pvc}_{date}.tar.gz"}
	name := b.formatName("prod", "myapp", "data-pvc")

	if !strings.HasPrefix(name, "prod_myapp_data-pvc_") {
		t.Errorf("formatName() = %q, want prefix %q", name, "prod_myapp_data-pvc_")
	}
	if !strings.HasSuffix(name, ".tar.gz") {
		t.Errorf("formatName() = %q, want suffix .tar.gz", name)
	}
}

func TestFormatName_Custom(t *testing.T) {
	b := &Backuper{outputFormat: "backup-{release}-{pvc}.tar.gz"}
	name := b.formatName("ns", "rel", "vol")
	if name != "backup-rel-vol.tar.gz" {
		t.Errorf("formatName() = %q, want %q", name, "backup-rel-vol.tar.gz")
	}
}

func TestCreateTarGz(t *testing.T) {
	// Create a temp source directory with files
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	subDir := filepath.Join(srcDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "file2.txt"), []byte("world"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create archive
	outDir := t.TempDir()
	archivePath := filepath.Join(outDir, "test.tar.gz")

	size, err := createTarGz(archivePath, srcDir)
	if err != nil {
		t.Fatalf("createTarGz() error: %v", err)
	}
	if size <= 0 {
		t.Errorf("size = %d, want > 0", size)
	}

	// Verify archive contents
	entries := readTarGzEntries(t, archivePath)
	expected := map[string]bool{
		".":             true,
		"file1.txt":     true,
		"subdir":        true,
		"subdir/file2.txt": true,
	}
	for _, e := range entries {
		if !expected[e] {
			t.Errorf("unexpected entry %q in archive", e)
		}
		delete(expected, e)
	}
	for e := range expected {
		t.Errorf("missing entry %q in archive", e)
	}
}

func TestCreateTarGz_FileContent(t *testing.T) {
	srcDir := t.TempDir()
	content := "test content 12345"
	if err := os.WriteFile(filepath.Join(srcDir, "data.txt"), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	outDir := t.TempDir()
	archivePath := filepath.Join(outDir, "test.tar.gz")

	_, err := createTarGz(archivePath, srcDir)
	if err != nil {
		t.Fatalf("createTarGz() error: %v", err)
	}

	// Read back and verify content
	got := readTarGzFileContent(t, archivePath, "data.txt")
	if got != content {
		t.Errorf("file content = %q, want %q", got, content)
	}
}

func TestBackupAll_NonexistentPath(t *testing.T) {
	outDir := t.TempDir()
	b := New(outDir, "{pvc}.tar.gz", false)

	pvcs := []types.PVCInfo{
		{PVCName: "test-pvc", HostPath: "/nonexistent/path/12345"},
	}

	results := b.BackupAll(pvcs, "ns", "rel")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Error("expected error for nonexistent path")
	}
}

func TestBackupAll_Success(t *testing.T) {
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "data.txt"), []byte("backup me"), 0644); err != nil {
		t.Fatal(err)
	}

	outDir := t.TempDir()
	b := New(outDir, "{pvc}.tar.gz", false)

	pvcs := []types.PVCInfo{
		{PVCName: "my-pvc", HostPath: srcDir},
	}

	results := b.BackupAll(pvcs, "ns", "rel")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	r := results[0]
	if r.Err != nil {
		t.Fatalf("unexpected error: %v", r.Err)
	}
	if r.PVCName != "my-pvc" {
		t.Errorf("PVCName = %q, want %q", r.PVCName, "my-pvc")
	}
	if r.Size <= 0 {
		t.Errorf("Size = %d, want > 0", r.Size)
	}
	if r.ArchivePath != filepath.Join(outDir, "my-pvc.tar.gz") {
		t.Errorf("ArchivePath = %q, want %q", r.ArchivePath, filepath.Join(outDir, "my-pvc.tar.gz"))
	}

	// Verify the file actually exists on disk
	if _, err := os.Stat(r.ArchivePath); err != nil {
		t.Errorf("archive file does not exist: %v", err)
	}
}

func TestBackupAll_MultipleePVCs(t *testing.T) {
	srcDir1 := t.TempDir()
	os.WriteFile(filepath.Join(srcDir1, "a.txt"), []byte("aaa"), 0644)
	srcDir2 := t.TempDir()
	os.WriteFile(filepath.Join(srcDir2, "b.txt"), []byte("bbb"), 0644)

	outDir := t.TempDir()
	b := New(outDir, "{pvc}.tar.gz", false)

	pvcs := []types.PVCInfo{
		{PVCName: "pvc-1", HostPath: srcDir1},
		{PVCName: "pvc-2", HostPath: srcDir2},
	}

	results := b.BackupAll(pvcs, "ns", "rel")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	for _, r := range results {
		if r.Err != nil {
			t.Errorf("PVC %s: unexpected error: %v", r.PVCName, r.Err)
		}
		if r.Size <= 0 {
			t.Errorf("PVC %s: size = %d, want > 0", r.PVCName, r.Size)
		}
	}
}

func TestBackupOne_NotADirectory(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "file.txt")
	os.WriteFile(tmpFile, []byte("not a dir"), 0644)

	outDir := t.TempDir()
	b := New(outDir, "{pvc}.tar.gz", false)

	pvcs := []types.PVCInfo{
		{PVCName: "test", HostPath: tmpFile},
	}

	results := b.BackupAll(pvcs, "ns", "rel")
	if results[0].Err == nil {
		t.Error("expected error when host path is not a directory")
	}
}

// --- helpers ---

func readTarGzEntries(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	var entries []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, hdr.Name)
	}
	return entries
}

func readTarGzFileContent(t *testing.T, archivePath, fileName string) string {
	t.Helper()
	f, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Name == fileName {
			data, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			return string(data)
		}
	}
	t.Fatalf("file %q not found in archive", fileName)
	return ""
}
