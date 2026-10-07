package archive_test

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pocketbase/pocketbase/tools/archive"
)

func TestVerify(t *testing.T) {
	t.Parallel()

	// prepare a valid source dir
	srcDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(srcDir, "storage", "a"), os.ModePerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "data.db"), []byte("SQLite format 3\x00 data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "auxiliary.db"), []byte("aux db"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "storage", "a", "file.txt"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	validZip := filepath.Join(t.TempDir(), "valid.zip")
	if err := archive.Create(srcDir, validZip); err != nil {
		t.Fatalf("failed to create test archive: %v", err)
	}

	t.Run("valid archive with all required files", func(t *testing.T) {
		if err := archive.Verify(validZip, "data.db", "auxiliary.db", "storage/a/file.txt"); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("valid archive without required files check", func(t *testing.T) {
		if err := archive.Verify(validZip); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("missing required file", func(t *testing.T) {
		err := archive.Verify(validZip, "data.db", "missing.db")
		if err == nil || !strings.Contains(err.Error(), `missing.db`) {
			t.Fatalf("expected missing required file error, got %v", err)
		}
	})

	t.Run("missing or non-zip file", func(t *testing.T) {
		invalidPath := filepath.Join(t.TempDir(), "invalid.zip")
		if err := os.WriteFile(invalidPath, []byte("not a zip"), 0644); err != nil {
			t.Fatal(err)
		}

		if err := archive.Verify(invalidPath); err == nil {
			t.Fatal("expected non-zip file to be rejected, got nil")
		}
	})

	t.Run("truncated archive", func(t *testing.T) {
		raw, err := os.ReadFile(validZip)
		if err != nil {
			t.Fatal(err)
		}

		truncatedPath := filepath.Join(t.TempDir(), "truncated.zip")
		if err := os.WriteFile(truncatedPath, raw[:len(raw)/2], 0644); err != nil {
			t.Fatal(err)
		}

		if err := archive.Verify(truncatedPath); err == nil {
			t.Fatal("expected truncated archive to be rejected, got nil")
		}
	})

	t.Run("corrupted entry content", func(t *testing.T) {
		corruptedPath := filepath.Join(t.TempDir(), "corrupted.zip")
		if err := createStoredZip(corruptedPath, map[string][]byte{
			"data.db": []byte("SQLite format 3\x00 data content"),
		}); err != nil {
			t.Fatal(err)
		}

		// flip a byte inside the stored entry data region
		// (30 bytes is the size of the local zip file header + 7 bytes name)
		raw, err := os.ReadFile(corruptedPath)
		if err != nil {
			t.Fatal(err)
		}
		raw[40] ^= 0xFF
		if err := os.WriteFile(corruptedPath, raw, 0644); err != nil {
			t.Fatal(err)
		}

		if err := archive.Verify(corruptedPath); err == nil {
			t.Fatal("expected CRC mismatch error, got nil")
		}
	})

	t.Run("zip slip entry", func(t *testing.T) {
		zipSlipPath := filepath.Join(t.TempDir(), "zipslip.zip")
		if err := createStoredZip(zipSlipPath, map[string][]byte{
			"../evil.txt": []byte("evil"),
		}); err != nil {
			t.Fatal(err)
		}

		err := archive.Verify(zipSlipPath)
		if err == nil || !strings.Contains(err.Error(), "invalid file path") {
			t.Fatalf("expected invalid file path error, got %v", err)
		}
	})
}

// createStoredZip is a test helper that creates a zip archive using the
// Store (no compression) method so that byte-level corruption can be
// deterministically simulated.
func createStoredZip(path string, files map[string][]byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	zw := zip.NewWriter(f)

	for name, content := range files {
		h := &zip.FileHeader{Name: name, Method: zip.Store}
		w, err := zw.CreateHeader(h)
		if err != nil {
			return err
		}
		if _, err := w.Write(content); err != nil {
			return err
		}
	}

	return zw.Close()
}
