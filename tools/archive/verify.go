package archive

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Verify performs a full integrity check of the zip archive at "src".
//
// Unlike just opening the archive (which only reads the central directory),
// Verify streams and fully reads every regular file entry from start to end,
// which means that:
//   - truncated or otherwise incomplete archives are detected
//     (missing/invalid central directory or prematurely ended data streams);
//   - every entry content is validated against its CRC-32 checksum;
//   - entries with unsafe paths (eg. Zip Slip) are rejected, mirroring [Extract].
//
// If one or more required file paths (matching the exact zip entry names)
// are expected, they must exist as regular files within the archive.
//
// Symbolic links, named pipes, sockets and other irregular entries are
// ignored the same way as in [Extract].
func Verify(src string, required ...string) error {
	zr, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer zr.Close()

	found := make(map[string]struct{}, len(required))

	for _, f := range zr.File {
		if !safeZipEntryName(f.Name) {
			return fmt.Errorf("invalid file path: %s", f.Name)
		}

		info := f.FileInfo()

		if info.IsDir() {
			continue
		}

		if !info.Mode().IsRegular() {
			// irregular entries are skipped by Extract too
			continue
		}

		if err := verifyZipEntry(f); err != nil {
			return fmt.Errorf("failed to verify %q: %w", f.Name, err)
		}

		found[f.Name] = struct{}{}
	}

	for _, name := range required {
		if _, ok := found[name]; !ok {
			return fmt.Errorf("required file %q is missing or invalid", name)
		}
	}

	return nil
}

// verifyZipEntry fully reads the entry content forcing the zip reader
// to validate its CRC-32 checksum at the end of the stream.
func verifyZipEntry(zipFile *zip.File) error {
	r, err := zipFile.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}

	return r.Close()
}

// safeZipEntryName mirrors the path checks performed by [extractFile]
// to ensure that the entry doesn't escape its destination directory.
func safeZipEntryName(name string) bool {
	basePath := filepath.Clean(string(os.PathSeparator)+"base") + string(os.PathSeparator)
	target := filepath.Join(basePath, name)

	return strings.HasPrefix(target, basePath)
}
