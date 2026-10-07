package core_test

import (
	"database/sql"
	"slices"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/list"
)

func execRaw(t *testing.T, app core.App, query string) {
	t.Helper()
	if _, err := app.DB().NewQuery(query).Execute(); err != nil {
		t.Fatalf("Failed to execute %q: %v", query, err)
	}
}

func findReport(r *core.ReconcileReport, name string) *core.ReconcileCollectionReport {
	for _, c := range r.Collections {
		if c.Collection.Name == name {
			return c
		}
	}
	return nil
}

// -------------------------------------------------------------------

func TestReconcileRecordTable_HealthyTableIsNoOp(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("reconcile_healthy")
	collection.Fields.Add(&core.TextField{Name: "note"})
	collection.AddIndex("idx_reconcile_healthy_note", false, "note", "")
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	record := core.NewRecord(collection)
	record.Set("note", "hello")
	if err := app.Save(record); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		report, err := app.ReconcileRecordTable(collection)
		if err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
		if report.HasChanges() {
			t.Fatalf("run %d should be no-op, got changes: %+v", i+1, report)
		}

		indexes, err := app.TableIndexes(collection.Name)
		if err != nil {
			t.Fatal(err)
		}
		if len(indexes) != 1 {
			t.Fatalf("Expected exactly 1 index, got %d: %v", len(indexes), indexes)
		}

		cols, err := app.TableColumns(collection.Name)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(cols, []string{"id", "note"}) {
			t.Fatalf("Expected columns [id note], got %v", cols)
		}
	}
}

// -------------------------------------------------------------------

func TestReconcileRecordTable_DriftConvergenceAndDataPreservation(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("reconcile_drift")
	collection.Fields.Add(&core.TextField{Name: "title"})
	collection.Fields.Add(&core.BoolField{Name: "active"})
	collection.AddIndex("idx_reconcile_drift_title", false, "title", "")
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	// sample records
	records := []struct {
		id     string
		title  string
		active bool
	}{
		{"rec000000000001", "first", true},
		{"rec000000000002", "second", false},
		{"rec000000000003", "third", true},
	}
	for _, s := range records {
		rec := core.NewRecord(collection)
		rec.Id = s.id
		rec.Set("title", s.title)
		rec.Set("active", s.active)
		if err := app.Save(rec); err != nil {
			t.Fatal(err)
		}
	}

	// introduce drift manually:
	// 1. defined "title" column is renamed (missing column + extra column with data)
	execRaw(t, app, `ALTER TABLE reconcile_drift RENAME COLUMN title TO title_gone`)

	// 2. completely unrelated extra column with data
	execRaw(t, app, `ALTER TABLE reconcile_drift ADD COLUMN junk TEXT`)
	execRaw(t, app, `UPDATE reconcile_drift SET junk = 'junk-' || id`)

	// 3. rebuild the table so that "active" becomes nullable (definition mismatch)
	execRaw(t, app, `DROP INDEX IF EXISTS idx_reconcile_drift_title`)
	execRaw(t, app, `
		CREATE TABLE reconcile_drift_tmp (
			id TEXT PRIMARY KEY,
			title_gone TEXT DEFAULT '',
			active BOOLEAN DEFAULT FALSE,
			junk TEXT
		)`)
	execRaw(t, app, `
		INSERT INTO reconcile_drift_tmp (id, title_gone, active, junk)
		SELECT id, title_gone, active, junk FROM reconcile_drift`)
	execRaw(t, app, `DROP TABLE reconcile_drift`)
	execRaw(t, app, `ALTER TABLE reconcile_drift_tmp RENAME TO reconcile_drift`)

	// 4. extra index (recreate since the table rebuild dropped it)
	execRaw(t, app, `CREATE INDEX idx_reconcile_drift_junk ON reconcile_drift(active)`)

	// run the reconciliation
	report, err := app.ReconcileRecordTable(collection)
	if err != nil {
		t.Fatal(err)
	}

	// report assertions
	if report.CreatedTable {
		t.Fatal("Expected the record table to be repaired, not recreated")
	}
	if !slices.Equal(report.AddedColumns, []string{"title"}) {
		t.Fatalf("Expected added [title], got %v", report.AddedColumns)
	}
	if !slices.Equal(report.DroppedColumns, []string{"junk", "title_gone"}) {
		t.Fatalf("Expected dropped [junk title_gone], got %v", report.DroppedColumns)
	}
	if !slices.Equal(report.AlteredColumns, []string{"active"}) {
		t.Fatalf("Expected altered [active], got %v", report.AlteredColumns)
	}
	if !slices.Equal(report.CreatedIndexes, []string{"idx_reconcile_drift_title"}) {
		t.Fatalf("Expected created [idx_reconcile_drift_title], got %v", report.CreatedIndexes)
	}
	if !slices.Equal(report.DroppedIndexes, []string{"idx_reconcile_drift_junk"}) {
		t.Fatalf("Expected dropped [idx_reconcile_drift_junk], got %v", report.DroppedIndexes)
	}

	// physical schema assertions
	cols, err := app.TableColumns(collection.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cols, []string{"id", "title", "active"}) {
		t.Fatalf("Expected columns [id title active], got %v", cols)
	}

	indexes, err := app.TableIndexes(collection.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d: %v", len(indexes), indexes)
	}

	// no record was lost
	count, err := app.CountRecords(collection)
	if err != nil {
		t.Fatal(err)
	}
	if count != int64(len(records)) {
		t.Fatalf("Expected %d records, got %d", len(records), count)
	}

	// values of the converged columns are preserved
	for _, s := range records {
		rec, err := app.FindRecordById(collection, s.id)
		if err != nil {
			t.Fatal(err)
		}
		if rec.GetBool("active") != s.active {
			t.Fatalf("Expected active %v for %s, got %v", s.active, s.id, rec.GetBool("active"))
		}
	}

	// raw values of the removed/mismatched columns are archived
	backupName := report.BackupTableName()
	if !app.HasTable(backupName) {
		t.Fatalf("Expected backup table %s to exist", backupName)
	}

	archived := []struct {
		Id        string `db:"id"`
		Junk      string `db:"junk"`
		TitleGone string `db:"title_gone"`
		Active    int    `db:"active"`
	}{}
	err = app.DB().Select("id", "junk", "title_gone", "active").
		From(backupName).OrderBy("id ASC").All(&archived)
	if err != nil {
		t.Fatal(err)
	}

	for i, s := range records {
		got := archived[i]
		if got.Id != s.id ||
			got.Junk != "junk-"+s.id ||
			got.TitleGone != s.title {
			t.Fatalf("Unexpected archived row: %+v (expected %s, junk-%s, %s)", got, s.id, s.id, s.title)
		}
	}

	// second run is a no-op (idempotency)
	report2, err := app.ReconcileRecordTable(collection)
	if err != nil {
		t.Fatal(err)
	}
	if report2.HasChanges() {
		t.Fatalf("Second run should be no-op, got: %+v", report2)
	}

	// the backup table itself is a collection-less table and must be left alone
	cols2, err := app.TableColumns(backupName)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols2) != 4 {
		t.Fatalf("Backup table columns changed: %v", cols2)
	}
}

// -------------------------------------------------------------------

func TestReconcileRecordTables_BulkIdempotencyAndUnrelatedTablesUntouched(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	// ordinary table that has nothing to do with any collection
	execRaw(t, app, `CREATE TABLE reconcile_custom_xyz (custom_id INTEGER PRIMARY KEY, custom_note TEXT)`)
	execRaw(t, app, `INSERT INTO reconcile_custom_xyz (custom_id, custom_note) VALUES (1, 'keep me'), (2, 'me too')`)

	first, err := app.ReconcileRecordTables()
	if err != nil {
		t.Fatalf("First bulk run failed: %v", err)
	}

	// views are skipped, never reconciled
	for _, c := range first.Collections {
		if c.Collection.IsView() && c.SkippedReason == "" {
			t.Fatalf("View %s should be skipped", c.Collection.Name)
		}
		if c.Collection.IsView() && c.HasChanges() {
			t.Fatalf("View %s should not be modified", c.Collection.Name)
		}
	}

	// the unrelated table is untouched
	customCols, err := app.TableColumns("reconcile_custom_xyz")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(customCols, []string{"custom_id", "custom_note"}) {
		t.Fatalf("Unrelated table columns changed: %v", customCols)
	}
	var customCount int
	if err := app.DB().Select("count(*)").From("reconcile_custom_xyz").Row(&customCount); err != nil {
		t.Fatal(err)
	}
	if customCount != 2 {
		t.Fatalf("Unrelated table rows changed: %d", customCount)
	}

	// second run converges nothing
	second, err := app.ReconcileRecordTables()
	if err != nil {
		t.Fatalf("Second bulk run failed: %v", err)
	}
	if second.HasChanges() {
		for _, c := range second.Collections {
			if c.HasChanges() {
				t.Fatalf("Second run should be no-op for %s: %+v", c.Collection.Name, c)
			}
		}
	}

	// unrelated table still untouched
	if err := app.DB().Select("count(*)").From("reconcile_custom_xyz").Row(&customCount); err != nil {
		t.Fatal(err)
	}
	if customCount != 2 {
		t.Fatalf("Unrelated table rows changed after rerun: %d", customCount)
	}
}

// -------------------------------------------------------------------

func TestReconcileRecordTables_ResumeAfterPartialRun(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	makeCollection := func(name string) *core.Collection {
		c := core.NewBaseCollection(name)
		c.Fields.Add(&core.TextField{Name: "note"})
		if err := app.Save(c); err != nil {
			t.Fatal(err)
		}
		for _, v := range []string{"a", "b"} {
			rec := core.NewRecord(c)
			rec.Set("note", v)
			if err := app.Save(rec); err != nil {
				t.Fatal(err)
			}
		}
		execRaw(t, app, `ALTER TABLE `+name+` ADD COLUMN stray TEXT`)
		execRaw(t, app, `UPDATE `+name+` SET stray = 'stray-' || id`)
		return c
	}

	a := makeCollection("reconcile_resume_a")
	b := makeCollection("reconcile_resume_b")

	// simulate a previous interrupted run that managed to finish only A
	if _, err := app.ReconcileRecordTables(a); err != nil {
		t.Fatal(err)
	}

	aCols, _ := app.TableColumns(a.Name)
	if list.ExistInSlice("stray", aCols) {
		t.Fatal("Expected A to be converged")
	}
	bCols, _ := app.TableColumns(b.Name)
	if !list.ExistInSlice("stray", bCols) {
		t.Fatal("Expected B to still be drifted")
	}

	// rerun: A is skipped (no changes), B converges
	result, err := app.ReconcileRecordTables()
	if err != nil {
		t.Fatal(err)
	}

	ar := findReport(result, a.Name)
	if ar == nil || ar.HasChanges() {
		t.Fatalf("Expected A report to be a no-op: %+v", ar)
	}
	br := findReport(result, b.Name)
	if br == nil || !slices.Equal(br.DroppedColumns, []string{"stray"}) {
		t.Fatalf("Expected B to drop stray: %+v", br)
	}

	// archived values of both collections are preserved
	for _, name := range []string{a.Name, b.Name} {
		var total int
		if err := app.DB().Select("count(*)").
			From("__pb_reconcile_backup_" + name).
			AndWhere(dbx.NewExp("[[stray]] IS NOT NULL")).
			Row(&total); err != nil {
			t.Fatal(err)
		}
		if total != 2 {
			t.Fatalf("Expected 2 archived rows for %s, got %d", name, total)
		}
	}
}

// -------------------------------------------------------------------

func TestReconcileRecordTable_ContinueWithPreexistingBackup(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("reconcile_continue")
	collection.Fields.Add(&core.TextField{Name: "note"})
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	rec1 := core.NewRecord(collection)
	rec1.Set("note", "one")
	if err := app.Save(rec1); err != nil {
		t.Fatal(err)
	}
	rec2 := core.NewRecord(collection)
	rec2.Set("note", "two")
	if err := app.Save(rec2); err != nil {
		t.Fatal(err)
	}

	execRaw(t, app, `ALTER TABLE reconcile_continue ADD COLUMN stray TEXT`)
	execRaw(t, app, `UPDATE reconcile_continue SET stray = 'live-' || id`)

	backupName := "__pb_reconcile_backup_reconcile_continue"

	// simulate a previous run that archived rec1 but didn't finish the convergence
	execRaw(t, app, `CREATE TABLE `+backupName+` (id TEXT PRIMARY KEY, stray TEXT)`)
	execRaw(t, app, `INSERT INTO `+backupName+` (id, stray) VALUES ('`+rec1.Id+`', 'manual-preserved')`)

	if _, err := app.ReconcileRecordTable(collection); err != nil {
		t.Fatal(err)
	}

	// previously archived value is preserved; missing rows are backfilled
	archived := []struct {
		Id    string         `db:"id"`
		Stray sql.NullString `db:"stray"`
	}{}
	if err := app.DB().Select("id", "stray").From(backupName).All(&archived); err != nil {
		t.Fatal(err)
	}

	archivedMap := map[string]sql.NullString{}
	for _, row := range archived {
		archivedMap[row.Id] = row.Stray
	}

	if len(archivedMap) != 2 {
		t.Fatalf("Expected 2 archived rows, got %d (%v)", len(archivedMap), archived)
	}
	if v := archivedMap[rec1.Id]; !v.Valid || v.String != "manual-preserved" {
		t.Fatalf("Expected preserved value to be kept, got %+v", v)
	}
	if v := archivedMap[rec2.Id]; !v.Valid || v.String != "live-"+rec2.Id {
		t.Fatalf("Expected rec2 to be backfilled, got %+v", v)
	}

	// live table converged
	cols, _ := app.TableColumns(collection.Name)
	if list.ExistInSlice("stray", cols) {
		t.Fatal("Expected stray to be dropped")
	}
}

// -------------------------------------------------------------------

func TestReconcileRecordTable_MissingTableIsCreated(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("reconcile_missing")
	collection.Fields.Add(&core.TextField{Name: "note"})
	collection.AddIndex("idx_reconcile_missing_note", false, "note", "")
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	// someone dropped the whole record table
	execRaw(t, app, `DROP TABLE reconcile_missing`)

	report, err := app.ReconcileRecordTable(collection)
	if err != nil {
		t.Fatal(err)
	}
	if !report.CreatedTable {
		t.Fatalf("Expected the table to be created: %+v", report)
	}
	if !slices.Equal(report.CreatedIndexes, []string{"idx_reconcile_missing_note"}) {
		t.Fatalf("Expected the declared index to be created: %v", report.CreatedIndexes)
	}

	cols, err := app.TableColumns(collection.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cols, []string{"id", "note"}) {
		t.Fatalf("Unexpected columns: %v", cols)
	}

	// rerun is a no-op
	report2, err := app.ReconcileRecordTable(collection)
	if err != nil {
		t.Fatal(err)
	}
	if report2.HasChanges() {
		t.Fatalf("Expected no-op, got %+v", report2)
	}
}

// -------------------------------------------------------------------

func TestReconcileRecordTable_InvalidDeclaredIndex(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("reconcile_badidx")
	collection.Fields.Add(&core.TextField{Name: "note"})
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	collection.Indexes = []string{"not even sql"}
	if err := app.UnsafeWithoutHooks().Save(collection); err != nil {
		t.Fatal(err)
	}

	if _, err := app.ReconcileRecordTable(collection); err == nil {
		t.Fatal("Expected an error for the invalid declared index")
	}
}
