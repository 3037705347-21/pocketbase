package core_test

import (
	"archive/zip"
	"bytes"
	"context"
	"testing"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
)

func TestReconcileBackups(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	ctx := context.Background()

	// create two valid backups
	for _, name := range []string{"test1.zip", "test2.zip"} {
		if err := app.CreateBackup(ctx, name); err != nil {
			t.Fatalf("Failed to create backup %q: %v", name, err)
		}
	}

	// first run - everything should be freshly verified and valid
	report, err := app.ReconcileBackups(ctx)
	if err != nil {
		t.Fatalf("Expected nil error, got %v", err)
	}

	if total := len(report.Entries); total != 2 {
		t.Fatalf("Expected 2 verified entries, got %d", total)
	}

	for _, entry := range report.Entries {
		if entry.Status != core.BackupVerificationValid {
			t.Fatalf("Expected %q to be valid, got %q (%s)", entry.Key, entry.Status, entry.Error)
		}
		if entry.Reused {
			t.Fatalf("Expected %q to be freshly verified", entry.Key)
		}
		if entry.CheckedAt.IsZero() {
			t.Fatalf("Expected non-zero checkedAt for %q", entry.Key)
		}
	}

	// the manifest should be persisted but not counted as a candidate
	fsys, err := app.NewBackupsFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()

	if exists, err := fsys.Exists(core.BackupVerificationManifestKey); err != nil || !exists {
		t.Fatalf("Expected persisted verification manifest, got exists=%v err=%v", exists, err)
	}

	// second run - results should be reused
	report2, err := app.ReconcileBackups(ctx)
	if err != nil {
		t.Fatalf("Expected nil error on rerun, got %v", err)
	}

	for _, entry := range report2.Entries {
		if !entry.Reused {
			t.Fatalf("Expected %q result to be reused", entry.Key)
		}
	}

	cached, _ := report.Find("test1.zip")
	cached2, _ := report2.Find("test1.zip")
	if !cached.CheckedAt.Equal(cached2.CheckedAt) {
		t.Fatalf("Expected reused checkedAt to be unchanged")
	}

	// entries are queryable separately
	storedEntries, err := app.FindBackupVerificationEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(storedEntries) != 2 {
		t.Fatalf("Expected 2 persisted entries, got %d", len(storedEntries))
	}

	// add a plain corrupt backup and a backup with invalid SQLite database
	if err := fsys.Upload([]byte("this is not a zip"), "broken.zip"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload(createZipWithBadDB(t), "baddb.zip"); err != nil {
		t.Fatal(err)
	}

	report3, err := app.ReconcileBackups(ctx)
	if err != nil {
		t.Fatalf("Expected nil error, got %v", err)
	}

	invalid := report3.Invalid()
	if len(invalid) != 2 {
		t.Fatalf("Expected 2 invalid entries, got %d", len(invalid))
	}

	for _, entry := range invalid {
		if entry.Error == "" {
			t.Fatalf("Expected failure reason for %q", entry.Key)
		}
		if entry.Reused {
			t.Fatalf("Expected %q to be freshly verified", entry.Key)
		}
	}

	// corrupt backups must be preserved (never auto-cleaned)
	for _, key := range []string{"broken.zip", "baddb.zip"} {
		if exists, _ := fsys.Exists(key); !exists {
			t.Fatalf("Expected corrupt backup %q to be preserved", key)
		}
	}

	// the valid entries should still be reused
	for _, key := range []string{"test1.zip", "test2.zip"} {
		entry, ok := report3.Find(key)
		if !ok || !entry.Reused || entry.Status != core.BackupVerificationValid {
			t.Fatalf("Expected %q to be reused and valid, got %+v", key, entry)
		}
	}

	// replace the corrupt backup with a valid one - fingerprint change
	// should force re-verification and flip the status
	if err := fsys.Delete("broken.zip"); err != nil {
		t.Fatal(err)
	}
	if err := app.CreateBackup(ctx, "broken.zip"); err != nil {
		t.Fatal(err)
	}

	report4, err := app.ReconcileBackups(ctx)
	if err != nil {
		t.Fatalf("Expected nil error, got %v", err)
	}

	fixed, ok := report4.Find("broken.zip")
	if !ok || fixed.Status != core.BackupVerificationValid || fixed.Reused {
		t.Fatalf("Expected replaced broken.zip to be freshly verified as valid, got %+v", fixed)
	}

	// deleting a backup should prune its persisted record on the next run
	if err := fsys.Delete("test2.zip"); err != nil {
		t.Fatal(err)
	}
	if _, err := app.ReconcileBackups(ctx); err != nil {
		t.Fatal(err)
	}

	stored, err := app.FindBackupVerificationEntries()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range stored {
		if entry.Key == "test2.zip" {
			t.Fatal("Expected the deleted backup verification record to be pruned")
		}
	}
}

func TestReconcileBackupsInterrupted(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	ctx := context.Background()

	if err := app.CreateBackup(ctx, "test1.zip"); err != nil {
		t.Fatal(err)
	}
	if err := app.CreateBackup(ctx, "test2.zip"); err != nil {
		t.Fatal(err)
	}

	// complete initial run
	if _, err := app.ReconcileBackups(ctx); err != nil {
		t.Fatal(err)
	}

	fsys, err := app.NewBackupsFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()

	if err := fsys.Upload([]byte("corrupt bytes"), "broken.zip"); err != nil {
		t.Fatal(err)
	}

	// interrupted run - it should fail but must not mark the pending backup
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := app.ReconcileBackups(canceledCtx); err == nil {
		t.Fatal("Expected interrupted reconciliation to return an error")
	}

	entries, err := app.FindBackupVerificationEntries()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Key == "broken.zip" {
			t.Fatal("The interrupted run must not persist a result for the pending backup")
		}
	}

	// resumed run - completed backups are reused, only the pending one is checked
	report, err := app.ReconcileBackups(ctx)
	if err != nil {
		t.Fatalf("Expected resumed run to succeed, got %v", err)
	}

	if total := len(report.Entries); total != 3 {
		t.Fatalf("Expected 3 entries, got %d", total)
	}

	for _, key := range []string{"test1.zip", "test2.zip"} {
		entry, ok := report.Find(key)
		if !ok || !entry.Reused {
			t.Fatalf("Expected %q to be reused after resume, got %+v", key, entry)
		}
	}

	pending, ok := report.Find("broken.zip")
	if !ok || pending.Reused || pending.Status != core.BackupVerificationInvalid {
		t.Fatalf("Expected broken.zip to be freshly verified as invalid, got %+v", pending)
	}
}

func TestReconcileBackupsGuard(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	app.Store().Set(core.StoreKeyActiveBackupVerification, struct{}{})

	_, err := app.ReconcileBackups(context.Background())
	if err == nil {
		t.Fatal("Expected guard error, got nil")
	}
}

// createZipWithBadDB builds a structurally valid zip with correct CRC
// checksums but containing a data.db file that is not a SQLite database.
func createZipWithBadDB(t *testing.T) []byte {
	t.Helper()

	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)

	w, err := zw.Create("data.db")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("not a sqlite database")); err != nil {
		t.Fatal(err)
	}

	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}
