package backup

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bitia-ru/k8s-hostpath-cloudflare-backup/pkg/types"
)

// Backuper creates tar.gz archives of PV host paths.
type Backuper struct {
	outputDir    string
	outputFormat string
	verbose      bool
}

func New(outputDir, outputFormat string, verbose bool) *Backuper {
	return &Backuper{
		outputDir:    outputDir,
		outputFormat: outputFormat,
		verbose:      verbose,
	}
}

// BackupAll creates archives for all given PVCs and returns results.
func (b *Backuper) BackupAll(pvcs []types.PVCInfo, namespace, release string) []types.BackupResult {
	var results []types.BackupResult
	for _, pvc := range pvcs {
		result := b.backupOne(pvc, namespace, release)
		results = append(results, result)
	}
	return results
}

func (b *Backuper) backupOne(pvc types.PVCInfo, namespace, release string) types.BackupResult {
	result := types.BackupResult{PVCName: pvc.PVCName}

	// Validate source path exists
	info, err := os.Stat(pvc.HostPath)
	if err != nil {
		result.Err = fmt.Errorf("host path %q: %w", pvc.HostPath, err)
		return result
	}
	if !info.IsDir() {
		result.Err = fmt.Errorf("host path %q is not a directory", pvc.HostPath)
		return result
	}

	archiveName := b.formatName(namespace, release, pvc.PVCName)
	archivePath := filepath.Join(b.outputDir, archiveName)
	result.ArchivePath = archivePath

	b.logf("Backing up %s -> %s", pvc.HostPath, archivePath)

	size, err := createTarGz(archivePath, pvc.HostPath)
	if err != nil {
		result.Err = fmt.Errorf("creating archive: %w", err)
		return result
	}

	result.Size = size
	b.logf("Created %s (%d bytes)", archivePath, size)
	return result
}

func FormatName(outputFormat, namespace, release, pvcName string) string {
	date := time.Now().Format("20060102-150405")
	name := outputFormat
	name = strings.ReplaceAll(name, "{namespace}", namespace)
	name = strings.ReplaceAll(name, "{release}", release)
	name = strings.ReplaceAll(name, "{pvc}", pvcName)
	name = strings.ReplaceAll(name, "{date}", date)
	return name
}

func (b *Backuper) formatName(namespace, release, pvcName string) string {
	return FormatName(b.outputFormat, namespace, release, pvcName)
}

func createTarGz(archivePath, sourceDir string) (int64, error) {
	file, err := os.Create(archivePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("creating tar header for %s: %w", path, err)
		}

		// Use relative path inside the archive
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		header.Name = relPath

		// Handle symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			header.Linkname = link
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("writing tar header: %w", err)
		}

		// Only write content for regular files
		if !info.Mode().IsRegular() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(tarWriter, f)
		return err
	})

	if err != nil {
		// Clean up partial archive on error
		os.Remove(archivePath)
		return 0, err
	}

	// Flush everything before getting file size
	tarWriter.Close()
	gzWriter.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// RestoreOne extracts a tar.gz archive into targetDir, clearing its contents first.
func (b *Backuper) RestoreOne(archivePath, targetDir string) error {
	b.logf("Restoring %s -> %s", archivePath, targetDir)

	// Validate target dir exists
	info, err := os.Stat(targetDir)
	if err != nil {
		return fmt.Errorf("target dir %q: %w", targetDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("target %q is not a directory", targetDir)
	}

	// Clear target dir contents
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return fmt.Errorf("reading target dir: %w", err)
	}
	for _, entry := range entries {
		p := filepath.Join(targetDir, entry.Name())
		b.logf("Removing %s", p)
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("clearing %s: %w", entry.Name(), err)
		}
	}

	// Open archive
	f, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("opening archive: %w", err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	cleanBase := filepath.Clean(targetDir)
	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar: %w", err)
		}

		target := filepath.Join(targetDir, hdr.Name)
		cleanTarget := filepath.Clean(target)

		// Prevent path traversal
		if cleanTarget != cleanBase && !strings.HasPrefix(cleanTarget, cleanBase+string(os.PathSeparator)) {
			return fmt.Errorf("illegal path in archive: %s", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return err
			}
			out.Close()
		case tar.TypeSymlink:
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		}
	}

	b.logf("Restored %s", targetDir)
	return nil
}

func (b *Backuper) logf(format string, args ...interface{}) {
	if b.verbose {
		log.Printf("[backup] "+format, args...)
	}
}
