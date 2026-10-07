package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func newPruneTestApp(t *testing.T) *BaseApp {
	t.Helper()

	app := NewBaseApp(BaseAppConfig{
		DataDir: t.TempDir(),
	})

	if err := app.Bootstrap(); err != nil {
		t.Fatalf("failed to bootstrap test app: %v", err)
	}

	t.Cleanup(func() {
		_ = app.ClearBootstrap()
	})

	return app
}

func listAutoBackupKeys(t *testing.T, app *BaseApp, prefix string) map[string]struct{} {
	t.Helper()

	fsys, err := app.NewBackupsFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()

	files, err := fsys.List(prefix)
	if err != nil {
		t.Fatal(err)
	}

	keys := map[string]struct{}{}
	for _, f := range files {
		if isBackupVerificationCandidate(f.Key) {
			keys[f.Key] = struct{}{}
		}
	}

	return keys
}

func TestPruneAutoBackupsOnlyVerifiedCountTowardQuota(t *testing.T) {
	ctx := context.Background()
	const prefix = "@auto_pb_backup_"

	t.Run("only newest verified backups are kept", func(t *testing.T) {
		app := newPruneTestApp(t)

		for i := 1; i <= 4; i++ {
			name := prefix + string(rune('0'+i)) + ".zip"
			if err := app.CreateBackup(ctx, name); err != nil {
				t.Fatalf("failed to create %q: %v", name, err)
			}
		}

		// a manual backup must be unaffected by the auto retention
		if err := app.CreateBackup(ctx, "manual.zip"); err != nil {
			t.Fatal(err)
		}

		report, err := app.ReconcileBackups(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if err := app.pruneAutoBackups(prefix, 2, report); err != nil {
			t.Fatal(err)
		}

		keys := listAutoBackupKeys(t, app, prefix)
		expected := []string{prefix + "3.zip", prefix + "4.zip"}
		if len(keys) != len(expected) {
			t.Fatalf("expected %d auto backups, got %d (%v)", len(expected), len(keys), keys)
		}
		for _, key := range expected {
			if _, ok := keys[key]; !ok {
				t.Fatalf("expected %q to be kept, got %v", key, keys)
			}
		}

		fsys, err := app.NewBackupsFilesystem()
		if err != nil {
			t.Fatal(err)
		}
		defer fsys.Close()
		if exists, _ := fsys.Exists("manual.zip"); !exists {
			t.Fatal("expected the manual backup to remain untouched")
		}
	})

	t.Run("corrupt backups are preserved and don't consume keep slots", func(t *testing.T) {
		app := newPruneTestApp(t)

		for i := 1; i <= 3; i++ {
			name := prefix + string(rune('0'+i)) + ".zip"
			if err := app.CreateBackup(ctx, name); err != nil {
				t.Fatalf("failed to create %q: %v", name, err)
			}
		}

		// simulate a newly created but incomplete/corrupt auto backup
		fsys, err := app.NewBackupsFilesystem()
		if err != nil {
			t.Fatal(err)
		}
		corruptKey := prefix + "4.zip"
		if err := fsys.Upload([]byte("truncated upload"), corruptKey); err != nil {
			t.Fatal(err)
		}
		fsys.Close()

		report, err := app.ReconcileBackups(ctx)
		if err != nil {
			t.Fatal(err)
		}

		corrupt, ok := report.Find(corruptKey)
		if !ok || corrupt.Status != BackupVerificationInvalid {
			t.Fatalf("expected %q to be marked invalid, got %+v", corruptKey, corrupt)
		}

		if err := app.pruneAutoBackups(prefix, 2, report); err != nil {
			t.Fatal(err)
		}

		keys := listAutoBackupKeys(t, app, prefix)

		// corrupt newest backup is kept, 2 newest valid are kept,
		// oldest valid (_1) is removed
		expected := []string{corruptKey, prefix + "2.zip", prefix + "3.zip"}
		if len(keys) != len(expected) {
			t.Fatalf("expected %d auto backups, got %d (%v)", len(expected), len(keys), keys)
		}
		for _, key := range expected {
			if _, ok := keys[key]; !ok {
				t.Fatalf("expected %q to be kept, got %v", key, keys)
			}
		}
		if _, ok := keys[prefix+"1.zip"]; ok {
			t.Fatal("did not expect the oldest valid backup to be kept")
		}
	})

	t.Run("unverified backups are not deleted", func(t *testing.T) {
		app := newPruneTestApp(t)

		for i := 1; i <= 3; i++ {
			name := prefix + string(rune('0'+i)) + ".zip"
			if err := app.CreateBackup(ctx, name); err != nil {
				t.Fatalf("failed to create %q: %v", name, err)
			}
		}

		// empty report simulates missing verification results
		emptyReport := &BackupVerificationReport{Entries: []BackupVerificationEntry{}}

		if err := app.pruneAutoBackups(prefix, 1, emptyReport); err != nil {
			t.Fatal(err)
		}

		keys := listAutoBackupKeys(t, app, prefix)
		if len(keys) != 3 {
			t.Fatalf("expected all 3 unverified backups to be kept, got %d (%v)", len(keys), keys)
		}
	})
}

func TestVerifyBackupObjectInconclusive(t *testing.T) {
	ctx := context.Background()
	app := newPruneTestApp(t)

	fsys, err := app.NewBackupsFilesystem()
	if err != nil {
		t.Fatal(err)
	}

	const key = "ghost.zip"
	if err := fsys.Upload([]byte("x"), key); err != nil {
		t.Fatal(err)
	}

	// simulate the backup data disappearing after being listed
	if err := os.Remove(filepath.Join(app.DataDir(), LocalBackupsDirName, key)); err != nil {
		t.Fatal(err)
	}

	status, errMsg, conclusive := app.verifyBackupObject(ctx, fsys, key, 1)
	if status != BackupVerificationInvalid {
		t.Fatalf("expected invalid status, got %q", status)
	}
	if errMsg == "" {
		t.Fatal("expected a failure reason")
	}
	if conclusive {
		t.Fatal("expected the missing-file failure to be inconclusive so that it gets retried")
	}
	fsys.Close()
}
