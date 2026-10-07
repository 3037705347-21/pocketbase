package core_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/filesystem"
	"github.com/pocketbase/pocketbase/tools/types"
)

const testReconcileCollectionName = "reconcile_test"

// testReconcileRecordId is a fixed valid record id (15 chars) used in the raw inserts.
const testReconcileRecordId = "reconrec0000001"

func setupReconcileApp(t *testing.T) *tests.TestApp {
	t.Helper()

	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatalf("failed to init test app: %v", err)
	}

	collection := core.NewBaseCollection(testReconcileCollectionName)
	collection.Fields.Add(
		&core.TextField{Name: "title"},
		&core.FileField{Name: "document"},
		&core.FileField{Name: "images", MaxSelect: 3, Thumbs: []string{"100x100"}},
	)
	if err := app.Save(collection); err != nil {
		t.Fatalf("failed to save test collection: %v", err)
	}

	return app
}

func reconcileCollection(app *tests.TestApp) *core.Collection {
	c, err := app.FindCachedCollectionByNameOrId(testReconcileCollectionName)
	if err != nil {
		panic(err)
	}
	return c
}

func reconcileRecordKey(app *tests.TestApp, filename string) string {
	return reconcileCollection(app).Id + "/" + testReconcileRecordId + "/" + filename
}

func assertFileExists(t *testing.T, fsys *filesystem.System, key string, expected bool) {
	t.Helper()

	exists, err := fsys.Exists(key)
	if err != nil {
		t.Fatalf("exists check failed for %q: %v", key, err)
	}
	if exists != expected {
		t.Fatalf("expected key %q exists = %v, got %v", key, expected, exists)
	}
}

func sha256Hex(content []byte) string {
	h := sha256.Sum256(content)
	return hex.EncodeToString(h[:])
}

// inserts a raw record row bypassing the file field interceptors,
// allowing to simulate files appearing in the storage without ownership.
func insertRawRecord(t *testing.T, app *tests.TestApp, document string, images []string) {
	t.Helper()

	if images == nil {
		images = []string{}
	}

	_, err := app.DB().Insert(testReconcileCollectionName, dbx.Params{
		"id":       testReconcileRecordId,
		"title":    "raw",
		"document": document,
		"images":   types.JSONArray[string](images),
	}).Execute()
	if err != nil {
		t.Fatalf("failed to insert raw record: %v", err)
	}
}

func insertOwnership(t *testing.T, app *tests.TestApp, m *core.FileOwnership) {
	t.Helper()

	if m.Id == "" {
		m.Id = sha256Hex([]byte(m.FileKey))
	}

	err := app.AuxDB().Model(m).Insert()
	if err != nil {
		t.Fatalf("failed to insert ownership row: %v", err)
	}
}

func findOwnership(t *testing.T, app *tests.TestApp, key string) *core.FileOwnership {
	t.Helper()

	m, err := app.FindFileOwnershipByKey(key)
	if err != nil {
		t.Fatalf("failed to find ownership for %q: %v", key, err)
	}
	return m
}

func reportOwnershipByKey(items []*core.FileOwnership, key string) *core.FileOwnership {
	for _, m := range items {
		if m.FileKey == key {
			return m
		}
	}
	return nil
}

func reportReferenceByKey(items []*core.FileReference, key string) *core.FileReference {
	for _, m := range items {
		if m.FileKey == key {
			return m
		}
	}
	return nil
}

func TestReconcileFilesAdoptsReferencedFiles(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	const filename = "existing_aaaa1111.txt"
	content := []byte("hello world")
	thumbContent := []byte("thumb")

	key := reconcileRecordKey(app, filename)
	thumbKey := filepath.ToSlash(filepath.Join(filepath.Dir(key), "thumbs_"+filename, "100x100_"+filename))

	// simulate a pre-feature file placed in the storage (+ a derived thumb)
	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload(content, key); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload(thumbContent, thumbKey); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	insertRawRecord(t, app, filename, nil)

	report, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{
		OrphanGracePeriod: -1,
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(report.Errors) > 0 {
		t.Fatalf("unexpected report errors: %v", report.Errors)
	}

	adopted := reportOwnershipByKey(report.Adopted, key)
	if adopted == nil {
		t.Fatalf("expected %q to be adopted (adopted: %+v)", key, report.Adopted)
	}
	if adopted.Digest != sha256Hex(content) {
		t.Fatalf("expected digest %q, got %q", sha256Hex(content), adopted.Digest)
	}
	if adopted.Size != int64(len(content)) {
		t.Fatalf("expected size %d, got %d", len(content), adopted.Size)
	}
	if len(adopted.Thumbs) != 1 || adopted.Thumbs[0] != thumbKey {
		t.Fatalf("expected thumbs [%s], got %v", thumbKey, adopted.Thumbs)
	}
	if adopted.Collection != reconcileCollection(app).Id || adopted.Record != testReconcileRecordId || adopted.Field != "document" {
		t.Fatalf("unexpected ownership attribution: %+v", adopted)
	}

	// should be persisted in the auxiliary db
	stored := findOwnership(t, app, key)
	if stored.Digest != adopted.Digest {
		t.Fatalf("persisted digest mismatch: %q vs %q", stored.Digest, adopted.Digest)
	}
	if len(stored.Thumbs) != 1 || stored.Thumbs[0] != thumbKey {
		t.Fatalf("persisted thumbs mismatch: %v", stored.Thumbs)
	}
}

func TestReconcileFilesIsIdempotent(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	const filename = "idem_bbbb2222.txt"
	key := reconcileRecordKey(app, filename)

	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("idem"), key); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	insertRawRecord(t, app, filename, nil)

	opts := &core.FileReconcileOptions{OrphanGracePeriod: -1}

	first, err := app.ReconcileFiles(context.Background(), opts)
	if err != nil || first.HasErrors() {
		t.Fatalf("first run failed: %v / %v", err, first.Errors)
	}
	if reportOwnershipByKey(first.Adopted, key) == nil {
		t.Fatalf("expected %q to be adopted, got %+v", key, first.Adopted)
	}

	second, err := app.ReconcileFiles(context.Background(), opts)
	if err != nil || second.HasErrors() {
		t.Fatalf("second run failed: %v / %v", err, second.Errors)
	}
	if len(second.Adopted) != 0 {
		t.Fatalf("expected 0 adoptions on rerun, got %d (%+v)", len(second.Adopted), second.Adopted)
	}
	if reportOwnershipByKey(second.Verified, key) == nil {
		t.Fatalf("expected the file to be verified, got %+v", second.Verified)
	}
	if reportOwnershipByKey(second.Orphans, key) != nil ||
		reportReferenceByKey(second.Missing, key) != nil {
		t.Fatalf("unexpected second run state for the key: %+v", second)
	}
}

func TestReconcileFilesExternalFilesAreUntouched(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	const referenced = "managed_cccc3333.txt"
	const externalInRecordDir = "external_dddd4444.txt"
	const externalTopLevel = "some_external_tool/eeee5555.bin"

	referencedKey := reconcileRecordKey(app, referenced)
	externalRecordKey := reconcileRecordKey(app, externalInRecordDir)

	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("ref"), referencedKey); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("external1"), externalRecordKey); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("external2"), externalTopLevel); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	insertRawRecord(t, app, referenced, nil)

	report, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{
		OrphanGracePeriod: -1,
	})
	if err != nil || report.HasErrors() {
		t.Fatalf("reconcile failed: %v / %v", err, report.Errors)
	}

	if reportOwnershipByKey(report.Adopted, referencedKey) == nil {
		t.Fatalf("expected the referenced file to be adopted, got %+v", report.Adopted)
	}

	found := map[string]bool{}
	for _, k := range report.External {
		found[k] = true
	}
	if !found[externalRecordKey] || !found[externalTopLevel] {
		t.Fatalf("expected external files to be reported, got %v", report.External)
	}

	// external files must remain in the storage
	fsys, err = app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()
	assertFileExists(t, fsys, externalRecordKey, true)
	assertFileExists(t, fsys, externalTopLevel, true)

	// and they must not have ownership records
	if _, err := app.FindFileOwnershipByKey(externalRecordKey); err == nil {
		t.Fatalf("expected no ownership record for the external in-record file")
	}
}

func TestReconcileFilesDeletesOrphanWithThumbs(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	orphanContent := []byte("orphan content")
	thumbContent := []byte("orphan thumb")

	orphanKey := reconcileRecordKey(app, "orphan_ffff6666.txt")
	thumbKey := filepath.ToSlash(filepath.Join(filepath.Dir(orphanKey), "thumbs_orphan_ffff6666.txt", "100x100_orphan_ffff6666.txt"))

	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload(orphanContent, orphanKey); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload(thumbContent, thumbKey); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	now := types.NowDateTime()
	insertOwnership(t, app, &core.FileOwnership{
		Collection: reconcileCollection(app).Id,
		Record:     testReconcileRecordId,
		Field:      "document",
		FileKey:    orphanKey,
		Name:       "orphan_ffff6666.txt",
		Digest:     sha256Hex(orphanContent),
		Size:       int64(len(orphanContent)),
		Thumbs:     types.JSONArray[string]{thumbKey},
		Created:    now.Add(-1 * time.Hour),
		Updated:    now.Add(-1 * time.Hour),
	})

	report, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{
		OrphanGracePeriod: -1,
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if report.HasErrors() {
		t.Fatalf("unexpected errors: %v", report.Errors)
	}

	if len(report.Orphans) != 1 || report.Orphans[0].FileKey != orphanKey {
		t.Fatalf("expected 1 orphan (%s), got %+v", orphanKey, report.Orphans)
	}

	fsys, err = app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	defer fsys.Close()

	assertFileExists(t, fsys, orphanKey, false)
	assertFileExists(t, fsys, thumbKey, false)

	if _, err := app.FindFileOwnershipByKey(orphanKey); err == nil {
		t.Fatalf("expected the orphan ownership record to be removed")
	}
}

func TestReconcileFilesPrunesStaleOwnershipRow(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	key := reconcileRecordKey(app, "already_gone_7777.txt")

	now := types.NowDateTime()
	insertOwnership(t, app, &core.FileOwnership{
		Collection: reconcileCollection(app).Id,
		Record:     testReconcileRecordId,
		Field:      "document",
		FileKey:    key,
		Name:       "already_gone_7777.txt",
		Digest:     sha256Hex([]byte("x")),
		Size:       1,
		Created:    now.Add(-1 * time.Hour),
		Updated:    now.Add(-1 * time.Hour),
	})

	report, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{
		OrphanGracePeriod: -1,
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if report.HasErrors() {
		t.Fatalf("unexpected errors: %v", report.Errors)
	}
	if len(report.Orphans) != 1 {
		t.Fatalf("expected stale row to be reported as orphan, got %+v", report.Orphans)
	}
	if _, err := app.FindFileOwnershipByKey(key); err == nil {
		t.Fatalf("expected stale ownership row to be pruned")
	}
}

func TestReconcileFilesReportsMissingReference(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	const missing = "ghost_8888.txt"
	key := reconcileRecordKey(app, missing)

	insertRawRecord(t, app, missing, nil)

	// ownership row exists but the file is gone (nothing should be deleted
	// because the record still references the file)
	now := types.NowDateTime()
	insertOwnership(t, app, &core.FileOwnership{
		Collection: reconcileCollection(app).Id,
		Record:     testReconcileRecordId,
		Field:      "document",
		FileKey:    key,
		Name:       missing,
		Digest:     sha256Hex([]byte("ghost")),
		Size:       5,
		Created:    now.Add(-time.Hour),
		Updated:    now.Add(-time.Hour),
	})

	report, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{
		OrphanGracePeriod: -1,
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if report.HasErrors() {
		t.Fatalf("unexpected errors: %v", report.Errors)
	}

	if len(report.Missing) != 1 || report.Missing[0].FileKey != key {
		t.Fatalf("expected the missing file to be reported, got %+v", report.Missing)
	}
	if len(report.Orphans) != 0 {
		t.Fatalf("missing referenced file must not be treated as orphan: %+v", report.Orphans)
	}

	// ownership record must be preserved so that the issue remains observable
	if m := findOwnership(t, app, key); m.Digest == "" {
		t.Fatalf("expected the ownership record to be preserved")
	}
}

func TestReconcileFilesReportsModifiedContent(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	const filename = "tampered_9999.txt"
	key := reconcileRecordKey(app, filename)

	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("v1"), key); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	insertRawRecord(t, app, filename, nil)

	first, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{OrphanGracePeriod: -1})
	if err != nil || first.HasErrors() {
		t.Fatalf("first run failed: %v / %v", err, first.Errors)
	}
	if reportOwnershipByKey(first.Adopted, key) == nil {
		t.Fatalf("expected %q to be adopted, got %+v", key, first.Adopted)
	}

	// simulate an external tool overwriting the file content
	fsys, err = app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("v2-tampered-content"), key); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	report, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{OrphanGracePeriod: -1})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if report.HasErrors() {
		t.Fatalf("unexpected errors: %v", report.Errors)
	}

	if len(report.Modified) != 1 {
		t.Fatalf("expected 1 modified file, got %+v", report.Modified)
	}
	issue := report.Modified[0]
	if issue.Ownership.FileKey != key {
		t.Fatalf("unexpected modified key: %q", issue.Ownership.FileKey)
	}
	if issue.StoredDigest != sha256Hex([]byte("v2-tampered-content")) {
		t.Fatalf("unexpected actual digest %q", issue.StoredDigest)
	}
	if issue.Reason != core.FileReconcileIssueModified {
		t.Fatalf("unexpected reason %q", issue.Reason)
	}

	// the tampered file must remain in place and the ledger digest untouched
	fsys, err = app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	assertFileExists(t, fsys, key, true)
	fsys.Close()

	stored := findOwnership(t, app, key)
	if stored.Digest != sha256Hex([]byte("v1")) {
		t.Fatalf("ledger digest must not be auto-updated, got %q", stored.Digest)
	}
}

func TestReconcileFilesDryRunDoesNotMutate(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	const adoptedName = "dry_aaaa.txt"
	const orphanName = "dry_bbbb.txt"

	referencedKey := reconcileRecordKey(app, adoptedName)
	orphanKey := reconcileRecordKey(app, orphanName)

	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("ref"), referencedKey); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("orph"), orphanKey); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	insertRawRecord(t, app, adoptedName, nil)

	now := types.NowDateTime()
	insertOwnership(t, app, &core.FileOwnership{
		Collection: reconcileCollection(app).Id,
		Record:     testReconcileRecordId,
		Field:      "document",
		FileKey:    orphanKey,
		Name:       orphanName,
		Digest:     sha256Hex([]byte("orph")),
		Size:       4,
		Created:    now.Add(-time.Hour),
		Updated:    now.Add(-time.Hour),
	})

	dry := &core.FileReconcileOptions{DryRun: true, OrphanGracePeriod: -1}
	report, err := app.ReconcileFiles(context.Background(), dry)
	if err != nil {
		t.Fatalf("dry run failed: %v", err)
	}
	if reportOwnershipByKey(report.Adopted, referencedKey) == nil {
		t.Fatalf("dry report should classify the referenced file as adopted: %+v", report.Adopted)
	}
	if reportOwnershipByKey(report.Orphans, orphanKey) == nil {
		t.Fatalf("dry report should classify the managed unreferenced file as orphan: %+v", report.Orphans)
	}

	// no ownership row should have been written for the adopted file
	if _, err := app.FindFileOwnershipByKey(referencedKey); err == nil {
		t.Fatalf("dry run must not create ownership records")
	}
	// orphan must still exist with its row
	fsys, err = app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	assertFileExists(t, fsys, orphanKey, true)
	fsys.Close()
	if m := findOwnership(t, app, orphanKey); m == nil {
		t.Fatalf("dry run must not remove orphan ownership rows")
	}

	// a real run should converge
	real, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{OrphanGracePeriod: -1})
	if err != nil || real.HasErrors() {
		t.Fatalf("real run failed: %v / %v", err, real.Errors)
	}
	if reportOwnershipByKey(real.Adopted, referencedKey) == nil {
		t.Fatalf("expected the referenced file to be adopted on the real run: %+v", real.Adopted)
	}
	if reportOwnershipByKey(real.Orphans, orphanKey) == nil {
		t.Fatalf("expected the orphan to be deleted on the real run: %+v", real.Orphans)
	}
}

func TestReconcileFilesAbortsWhenStorageUnreadable(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	orphanKey := reconcileRecordKey(app, "no_delete_cccc.txt")

	local, err := filesystem.NewLocal(filepath.Join(app.DataDir(), "storage"))
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Upload([]byte("orphan"), orphanKey); err != nil {
		t.Fatal(err)
	}
	local.Close()

	now := types.NowDateTime()
	insertOwnership(t, app, &core.FileOwnership{
		Collection: reconcileCollection(app).Id,
		Record:     testReconcileRecordId,
		Field:      "document",
		FileKey:    orphanKey,
		Name:       "no_delete_cccc.txt",
		Digest:     sha256Hex([]byte("orphan")),
		Size:       6,
		Created:    now.Add(-time.Hour),
		Updated:    now.Add(-time.Hour),
	})

	// point the app to an unreachable S3 storage
	app.Settings().S3.Enabled = true
	app.Settings().S3.Bucket = "test"
	app.Settings().S3.Region = "us-east-1"
	app.Settings().S3.Endpoint = "http://127.0.0.1:9"
	app.Settings().S3.AccessKey = "test"
	app.Settings().S3.Secret = "test"
	app.Settings().S3.ForcePathStyle = true

	report, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{
		OrphanGracePeriod: -1,
	})
	if err == nil {
		t.Fatalf("expected reconcile to fail when the storage is unreadable")
	}
	if report != nil && len(report.Orphans) > 0 {
		t.Fatalf("no orphan must be confirmed when storage cannot be read: %+v", report.Orphans)
	}

	// nothing must have been deleted from the (local) storage
	local, err = filesystem.NewLocal(filepath.Join(app.DataDir(), "storage"))
	if err != nil {
		t.Fatal(err)
	}
	assertFileExists(t, local, orphanKey, true)
	local.Close()

	// and the ownership record must be still there
	if m := findOwnership(t, app, orphanKey); m == nil {
		t.Fatalf("ownership row must be preserved on unreadable storage")
	}
}

func TestReconcileFilesResumesAfterInterruptedDeletion(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	orphanContent := []byte("orphan")
	orphanKey := reconcileRecordKey(app, "interrupt_dddd.txt")
	thumbKey := filepath.ToSlash(filepath.Join(filepath.Dir(orphanKey), "thumbs_interrupt_dddd.txt", "100x100_interrupt_dddd.txt"))

	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload(orphanContent, orphanKey); err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("thumb"), thumbKey); err != nil {
		t.Fatal(err)
	}

	// simulate a crash after the original was deleted but before the thumbs/row cleanup
	if err := fsys.Delete(orphanKey); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	now := types.NowDateTime()
	insertOwnership(t, app, &core.FileOwnership{
		Collection: reconcileCollection(app).Id,
		Record:     testReconcileRecordId,
		Field:      "document",
		FileKey:    orphanKey,
		Name:       "interrupt_dddd.txt",
		Digest:     sha256Hex(orphanContent),
		Size:       int64(len(orphanContent)),
		Thumbs:     types.JSONArray[string]{thumbKey},
		Created:    now.Add(-time.Hour),
		Updated:    now.Add(-time.Hour),
	})

	report, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{OrphanGracePeriod: -1})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if report.HasErrors() {
		t.Fatalf("unexpected errors: %v", report.Errors)
	}
	if len(report.Orphans) != 1 {
		t.Fatalf("expected the interrupted orphan to converge, got %+v", report.Orphans)
	}

	fsys, err = app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	assertFileExists(t, fsys, thumbKey, false)
	fsys.Close()

	if _, err := app.FindFileOwnershipByKey(orphanKey); err == nil {
		t.Fatalf("expected ownership row to be removed")
	}

	// rerun - fully converged
	second, err := app.ReconcileFiles(context.Background(), &core.FileReconcileOptions{OrphanGracePeriod: -1})
	if err != nil || second.HasErrors() {
		t.Fatalf("rerun failed: %v / %v", err, second.Errors)
	}
	if len(second.Orphans) != 0 {
		t.Fatalf("expected no orphans after convergence, got %+v", second.Orphans)
	}
}

func TestReconcileFilesGracePeriodSkipsFreshOwnership(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	orphanKey := reconcileRecordKey(app, "fresh_eeee.txt")

	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("fresh"), orphanKey); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	// ownership created "just now" -> potentially in-flight
	insertOwnership(t, app, &core.FileOwnership{
		Collection: reconcileCollection(app).Id,
		Record:     testReconcileRecordId,
		Field:      "document",
		FileKey:    orphanKey,
		Name:       "fresh_eeee.txt",
		Digest:     sha256Hex([]byte("fresh")),
		Size:       5,
		Created:    types.NowDateTime(),
		Updated:    types.NowDateTime(),
	})

	report, err := app.ReconcileFiles(context.Background()) // default grace
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(report.Pending) != 1 || report.Pending[0].FileKey != orphanKey {
		t.Fatalf("expected the fresh orphan to be pending, got orphans=%+v pending=%+v", report.Orphans, report.Pending)
	}
	if len(report.Orphans) != 0 {
		t.Fatalf("fresh ownership must not be deleted within the grace period: %+v", report.Orphans)
	}

	fsys, err = app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	assertFileExists(t, fsys, orphanKey, true)
	fsys.Close()
}

func TestReconcileFilesLiveLifecycleIntegration(t *testing.T) {
	app := setupReconcileApp(t)
	defer app.Cleanup()

	collection := reconcileCollection(app)

	// 1. upload through the record lifecycle registers ownership automatically
	f1, err := filesystem.NewFileFromBytes([]byte("file1"), "doc1.txt")
	if err != nil {
		t.Fatal(err)
	}

	record := core.NewRecord(collection)
	record.Set("id", testReconcileRecordId)
	record.Set("document", f1)
	if err := app.Save(record); err != nil {
		t.Fatalf("failed to create record: %v", err)
	}

	key1 := record.BaseFilesPath() + "/" + f1.Name
	m1 := findOwnership(t, app, key1)
	if m1.Digest != sha256Hex([]byte("file1")) || m1.Field != "document" ||
		m1.Collection != collection.Id || m1.Record != record.Id {
		t.Fatalf("unexpected live ownership record: %+v", m1)
	}

	// 2. a thumb appears and the next reconcile tracks it
	thumbKey := record.BaseFilesPath() + "/thumbs_" + f1.Name + "/100x100_" + f1.Name
	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.Upload([]byte("thumbbytes"), thumbKey); err != nil {
		t.Fatal(err)
	}
	fsys.Close()

	report, err := app.ReconcileFiles(context.Background())
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if report.HasErrors() {
		t.Fatalf("unexpected errors: %v", report.Errors)
	}
	m1 = findOwnership(t, app, key1)
	if len(m1.Thumbs) != 1 || m1.Thumbs[0] != thumbKey {
		t.Fatalf("expected reconcile to track the thumb, got %v", m1.Thumbs)
	}

	// 3. replacing the file deletes the old file, its thumbs and unregisters ownership
	f2, err := filesystem.NewFileFromBytes([]byte("file2-longer"), "doc2.txt")
	if err != nil {
		t.Fatal(err)
	}
	record.Set("document", f2)
	if err := app.Save(record); err != nil {
		t.Fatalf("failed to update record: %v", err)
	}
	key2 := record.BaseFilesPath() + "/" + f2.Name

	fsys, err = app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}
	assertFileExists(t, fsys, key1, false)
	assertFileExists(t, fsys, thumbKey, false)
	assertFileExists(t, fsys, key2, true)
	fsys.Close()

	if _, err := app.FindFileOwnershipByKey(key1); err == nil {
		t.Fatalf("expected old ownership row to be removed")
	}
	m2 := findOwnership(t, app, key2)
	if m2.Digest != sha256Hex([]byte("file2-longer")) {
		t.Fatalf("unexpected new ownership digest: %q", m2.Digest)
	}
}
