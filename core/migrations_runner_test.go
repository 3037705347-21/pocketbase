package core_test

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
)

func TestMigrationsRunnerUpAndDown(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	callsOrder := []string{}

	l := core.MigrationsList{}
	l.Register(func(app core.App) error {
		callsOrder = append(callsOrder, "up2")
		return nil
	}, func(app core.App) error {
		callsOrder = append(callsOrder, "down2")
		return nil
	}, "2_test")
	l.Register(func(app core.App) error {
		callsOrder = append(callsOrder, "up3")
		return nil
	}, func(app core.App) error {
		callsOrder = append(callsOrder, "down3")
		return nil
	}, "3_test")
	l.Register(func(app core.App) error {
		callsOrder = append(callsOrder, "up1")
		return nil
	}, func(app core.App) error {
		callsOrder = append(callsOrder, "down1")
		return nil
	}, "1_test")
	l.Register(func(app core.App) error {
		callsOrder = append(callsOrder, "up4")
		return nil
	}, func(app core.App) error {
		callsOrder = append(callsOrder, "down4")
		return nil
	}, "4_test")
	l.Add(&core.Migration{
		Up: func(app core.App) error {
			callsOrder = append(callsOrder, "up5")
			return nil
		},
		Down: func(app core.App) error {
			callsOrder = append(callsOrder, "down5")
			return nil
		},
		File: "5_test",
		ReapplyCondition: func(txApp core.App, runner *core.MigrationsRunner, fileName string) (bool, error) {
			return true, nil
		},
	})

	runner := core.NewMigrationsRunner(app, l)

	// ---------------------------------------------------------------
	// simulate partially out-of-order applied migration
	// ---------------------------------------------------------------

	_, err := app.DB().Insert(core.DefaultMigrationsTable, dbx.Params{
		"file":    "4_test",
		"applied": time.Now().UnixMicro() - 2,
	}).Execute()
	if err != nil {
		t.Fatalf("Failed to insert 5_test migration: %v", err)
	}

	_, err = app.DB().Insert(core.DefaultMigrationsTable, dbx.Params{
		"file":    "5_test",
		"applied": time.Now().UnixMicro() - 1,
	}).Execute()
	if err != nil {
		t.Fatalf("Failed to insert 5_test migration: %v", err)
	}

	_, err = app.DB().Insert(core.DefaultMigrationsTable, dbx.Params{
		"file":    "2_test",
		"applied": time.Now().UnixMicro(),
	}).Execute()
	if err != nil {
		t.Fatalf("Failed to insert 2_test migration: %v", err)
	}

	// ---------------------------------------------------------------
	// Up()
	// ---------------------------------------------------------------

	if _, err := runner.Up(); err != nil {
		t.Fatal(err)
	}

	expectedUpCallsOrder := `["up1","up3","up5"]` // skip up2 and up4 since they were applied already (up5 has extra reapply condition)

	upCallsOrder, err := json.Marshal(callsOrder)
	if err != nil {
		t.Fatal(err)
	}

	if v := string(upCallsOrder); v != expectedUpCallsOrder {
		t.Fatalf("Expected Up() calls order %s, got %s", expectedUpCallsOrder, upCallsOrder)
	}

	// ---------------------------------------------------------------

	// reset callsOrder
	callsOrder = []string{}

	// simulate unrun migration
	l.Register(nil, func(app core.App) error {
		callsOrder = append(callsOrder, "down6")
		return nil
	}, "6_test")

	// simulate applied migrations from different migrations list
	_, err = app.DB().Insert(core.DefaultMigrationsTable, dbx.Params{
		"file":    "from_different_list",
		"applied": time.Now().UnixMicro(),
	}).Execute()
	if err != nil {
		t.Fatalf("Failed to insert from_different_list migration: %v", err)
	}

	// ---------------------------------------------------------------

	// ---------------------------------------------------------------
	// Down()
	// ---------------------------------------------------------------

	if _, err := runner.Down(2); err != nil {
		t.Fatal(err)
	}

	expectedDownCallsOrder := `["down5","down3"]` // revert in the applied order

	downCallsOrder, err := json.Marshal(callsOrder)
	if err != nil {
		t.Fatal(err)
	}

	if v := string(downCallsOrder); v != expectedDownCallsOrder {
		t.Fatalf("Expected Down() calls order %s, got %s", expectedDownCallsOrder, downCallsOrder)
	}
}

func TestMigrationsRunnerRemoveMissingAppliedMigrations(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	// mock migrations history
	for i := 1; i <= 3; i++ {
		_, err := app.DB().Insert(core.DefaultMigrationsTable, dbx.Params{
			"file":    fmt.Sprintf("%d_test", i),
			"applied": time.Now().UnixMicro(),
		}).Execute()
		if err != nil {
			t.Fatal(err)
		}
	}

	if !isMigrationApplied(app, "2_test") {
		t.Fatalf("Expected 2_test migration to be applied")
	}

	// create a runner without 2_test to mock deleted migration
	l := core.MigrationsList{}
	l.Register(func(app core.App) error {
		return nil
	}, func(app core.App) error {
		return nil
	}, "1_test")
	l.Register(func(app core.App) error {
		return nil
	}, func(app core.App) error {
		return nil
	}, "3_test")

	r := core.NewMigrationsRunner(app, l)

	if err := r.RemoveMissingAppliedMigrations(); err != nil {
		t.Fatalf("Failed to remove missing applied migrations: %v", err)
	}

	if isMigrationApplied(app, "2_test") {
		t.Fatalf("Expected 2_test migration to NOT be applied")
	}
}

func isMigrationApplied(app core.App, file string) bool {
	var exists int

	err := app.DB().Select("(1)").
		From(core.DefaultMigrationsTable).
		Where(dbx.HashExp{"file": file}).
		Limit(1).
		Row(&exists)

	return err == nil && exists > 0
}

func TestMigrationIdentity(t *testing.T) {
	t.Parallel()

	a := core.MigrationIdentity([]byte("migrate((app) => { app.log('a') })\n"))
	b := core.MigrationIdentity([]byte("migrate((app) => { app.log('b') })\n"))

	if a == "" || a == b {
		t.Fatalf("Expected distinct non-empty identities, got %q and %q", a, b)
	}

	// CRLF and lone CR checkouts should produce the same identity
	crlf := core.MigrationIdentity([]byte("line1\r\nline2\r\n"))
	lf := core.MigrationIdentity([]byte("line1\nline2\n"))
	cr := core.MigrationIdentity([]byte("line1\rline2\r"))

	if crlf != lf || cr != lf {
		t.Fatalf("Expected line-ending-normalized identities to match, got %q, %q and %q", crlf, lf, cr)
	}

	if len(a) != 64 {
		t.Fatalf("Expected a 64 chars sha256 hex identity, got %d (%q)", len(a), a)
	}
}

type testHistorySeed struct {
	file     string
	identity string
	applied  int64
}

type testHistoryRowState struct {
	file     string
	identity string
	applied  int64
}

type testReconcileSource struct {
	file     string
	identity string
}

func initTestMigrationsTable(t *testing.T, app core.App) {
	t.Helper()
	if _, err := core.NewMigrationsRunner(app, core.MigrationsList{}).Up(); err != nil {
		t.Fatalf("Failed to initialize migrations table: %v", err)
	}
}

func resetTestMigrationsHistory(t *testing.T, app core.App) {
	t.Helper()
	initTestMigrationsTable(t, app)

	if _, err := app.DB().NewQuery("DELETE FROM " + core.DefaultMigrationsTable).Execute(); err != nil {
		t.Fatalf("Failed to clear migrations history: %v", err)
	}
}

func seedTestMigrationsHistory(t *testing.T, app core.App, seeds ...testHistorySeed) {
	t.Helper()
	for _, seed := range seeds {
		_, err := app.DB().Insert(core.DefaultMigrationsTable, dbx.Params{
			"file":     seed.file,
			"applied":  seed.applied,
			"identity": seed.identity,
		}).Execute()
		if err != nil {
			t.Fatalf("Failed to insert %q history entry: %v", seed.file, err)
		}
	}
}

func allTestMigrationsHistory(t *testing.T, app core.App) []testHistoryRowState {
	t.Helper()

	rows := []struct {
		File     string `db:"file"`
		Applied  int64  `db:"applied"`
		Identity string `db:"identity"`
	}{}

	err := app.DB().Select("file", "applied", "identity").
		From(core.DefaultMigrationsTable).
		OrderBy("file ASC").
		All(&rows)
	if err != nil {
		t.Fatalf("Failed to fetch migrations history: %v", err)
	}

	result := make([]testHistoryRowState, 0, len(rows))
	for _, row := range rows {
		result = append(result, testHistoryRowState{file: row.File, identity: row.Identity, applied: row.Applied})
	}

	return result
}

func newTestReconcileList(sources ...testReconcileSource) core.MigrationsList {
	var list core.MigrationsList
	for _, source := range sources {
		list.Add(&core.Migration{File: source.file, Identity: source.identity})
	}
	return list
}

func TestMigrationsRunnerReconcileAppliedMigrations(t *testing.T) {
	t.Parallel()

	scenarios := []struct {
		name             string
		history          []testHistorySeed
		sources          []testReconcileSource
		expectedStatuses map[string]string // history file -> status
		expectedRenames  map[string]string // history file -> new file
		expectedHistory  []testHistoryRowState
	}{
		{
			name:             "consistent identity and name",
			history:          []testHistorySeed{{file: "1_a.js", identity: "idA", applied: 100}},
			sources:          []testReconcileSource{{file: "1_a.js", identity: "idA"}},
			expectedStatuses: map[string]string{"1_a.js": core.MigrationHistoryConsistent},
			expectedHistory:  []testHistoryRowState{{file: "1_a.js", identity: "idA", applied: 100}},
		},
		{
			name:             "consistent legacy entry with identity backfill",
			history:          []testHistorySeed{{file: "1_a.js", identity: "", applied: 101}},
			sources:          []testReconcileSource{{file: "1_a.js", identity: "idA"}},
			expectedStatuses: map[string]string{"1_a.js": core.MigrationHistoryConsistent},
			expectedHistory:  []testHistoryRowState{{file: "1_a.js", identity: "idA", applied: 101}},
		},
		{
			name:             "consistent without any identity",
			history:          []testHistorySeed{{file: "1_a.go", identity: "", applied: 102}},
			sources:          []testReconcileSource{{file: "1_a.go", identity: ""}},
			expectedStatuses: map[string]string{"1_a.go": core.MigrationHistoryConsistent},
			expectedHistory:  []testHistoryRowState{{file: "1_a.go", identity: "", applied: 102}},
		},
		{
			name:             "renamed file with same content",
			history:          []testHistorySeed{{file: "1_old_name.js", identity: "idA", applied: 200}},
			sources:          []testReconcileSource{{file: "2_new_name.js", identity: "idA"}},
			expectedStatuses: map[string]string{"1_old_name.js": core.MigrationHistoryRenamed},
			expectedRenames:  map[string]string{"1_old_name.js": "2_new_name.js"},
			expectedHistory:  []testHistoryRowState{{file: "2_new_name.js", identity: "idA", applied: 200}},
		},
		{
			name: "renamed file and old name reused by new content",
			history: []testHistorySeed{
				{file: "1_a.js", identity: "id1", applied: 201},
			},
			sources: []testReconcileSource{
				{file: "1_a.js", identity: "id2"},
				{file: "2_b.js", identity: "id1"},
			},
			expectedStatuses: map[string]string{"1_a.js": core.MigrationHistoryRenamed},
			expectedRenames:  map[string]string{"1_a.js": "2_b.js"},
			expectedHistory:  []testHistoryRowState{{file: "2_b.js", identity: "id1", applied: 201}},
		},
		{
			name:             "content changed same name",
			history:          []testHistorySeed{{file: "1_a.js", identity: "id1", applied: 300}},
			sources:          []testReconcileSource{{file: "1_a.js", identity: "id2"}},
			expectedStatuses: map[string]string{"1_a.js": core.MigrationHistoryContentChanged},
			expectedHistory:  []testHistoryRowState{{file: "1_a.js", identity: "id2", applied: 300}},
		},
		{
			name:             "source disappeared with identity",
			history:          []testHistorySeed{{file: "1_gone.js", identity: "idG", applied: 400}},
			sources:          nil,
			expectedStatuses: map[string]string{"1_gone.js": core.MigrationHistoryMissing},
			expectedHistory:  nil,
		},
		{
			name:             "legacy source disappeared without candidates",
			history:          []testHistorySeed{{file: "1_gone.js", identity: "", applied: 401}},
			sources:          nil,
			expectedStatuses: map[string]string{"1_gone.js": core.MigrationHistoryMissing},
			expectedHistory:  nil,
		},
		{
			name: "cyclic rename a<->b",
			history: []testHistorySeed{
				{file: "1_a.js", identity: "idA", applied: 500},
				{file: "2_b.js", identity: "idB", applied: 501},
			},
			sources: []testReconcileSource{
				{file: "1_a.js", identity: "idB"},
				{file: "2_b.js", identity: "idA"},
			},
			expectedStatuses: map[string]string{
				"1_a.js": core.MigrationHistoryRenamed,
				"2_b.js": core.MigrationHistoryRenamed,
			},
			expectedRenames: map[string]string{
				"1_a.js": "2_b.js",
				"2_b.js": "1_a.js",
			},
			expectedHistory: []testHistoryRowState{
				{file: "1_a.js", identity: "idB", applied: 501},
				{file: "2_b.js", identity: "idA", applied: 500},
			},
		},
		{
			name: "mixed batch",
			history: []testHistorySeed{
				{file: "1_keep.js", identity: "idK", applied: 600},
				{file: "2_old.js", identity: "idR", applied: 601},
				{file: "3_gone.js", identity: "idG", applied: 602},
				{file: "4_edit.js", identity: "id1", applied: 603},
			},
			sources: []testReconcileSource{
				{file: "1_keep.js", identity: "idK"},
				{file: "2_renamed.js", identity: "idR"},
				{file: "4_edit.js", identity: "id2"},
			},
			expectedStatuses: map[string]string{
				"1_keep.js": core.MigrationHistoryConsistent,
				"2_old.js":  core.MigrationHistoryRenamed,
				"3_gone.js": core.MigrationHistoryMissing,
				"4_edit.js": core.MigrationHistoryContentChanged,
			},
			expectedRenames: map[string]string{"2_old.js": "2_renamed.js"},
			expectedHistory: []testHistoryRowState{
				{file: "1_keep.js", identity: "idK", applied: 600},
				{file: "2_renamed.js", identity: "idR", applied: 601},
				{file: "4_edit.js", identity: "id2", applied: 603},
			},
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			app, _ := tests.NewTestApp()
			defer app.Cleanup()

			resetTestMigrationsHistory(t, app)
			seedTestMigrationsHistory(t, app, s.history...)

			runner := core.NewMigrationsRunner(app, newTestReconcileList(s.sources...))

			result, err := runner.ReconcileAppliedMigrations()
			if err != nil {
				t.Fatalf("ReconcileAppliedMigrations failed: %v", err)
			}

			if len(result.Items) != len(s.expectedStatuses) {
				t.Fatalf("Expected %d reconciled items, got %d: %+v", len(s.expectedStatuses), len(result.Items), result.Items)
			}

			for _, item := range result.Items {
				expectedStatus, ok := s.expectedStatuses[item.File]
				if !ok {
					t.Fatalf("Unexpected reconciled item %q", item.File)
				}
				if item.Status != expectedStatus {
					t.Fatalf("Expected %q to be %q, got %q", item.File, expectedStatus, item.Status)
				}

				if expectedRename := s.expectedRenames[item.File]; expectedRename != "" && item.NewFile != expectedRename {
					t.Fatalf("Expected %q to be renamed to %q, got %q", item.File, expectedRename, item.NewFile)
				}
			}

			gotHistory := allTestMigrationsHistory(t, app)
			if len(gotHistory) != len(s.expectedHistory) {
				t.Fatalf("Expected %d history rows after reconcile, got %d: %+v", len(s.expectedHistory), len(gotHistory), gotHistory)
			}

			for i, expected := range s.expectedHistory {
				if gotHistory[i] != expected {
					t.Fatalf("Expected history row %d to be %+v, got %+v", i, expected, gotHistory[i])
				}
			}
		})
	}
}

func TestMigrationsRunnerReconcileAppliedMigrationsUndeterminable(t *testing.T) {
	t.Parallel()

	scenarios := []struct {
		name    string
		history []testHistorySeed
		sources []testReconcileSource
	}{
		{
			name:    "legacy missing entry with unapplied candidate",
			history: []testHistorySeed{{file: "1_old.js", identity: "", applied: 100}},
			sources: []testReconcileSource{{file: "2_new.js", identity: "idN"}},
		},
		{
			name:    "identity matching multiple sources",
			history: []testHistorySeed{{file: "1_a.js", identity: "id1", applied: 100}},
			sources: []testReconcileSource{{file: "2_b.js", identity: "id1"}, {file: "3_c.js", identity: "id1"}},
		},
		{
			name: "two applied entries mapping to the same target",
			history: []testHistorySeed{
				{file: "1_a.js", identity: "id1", applied: 100},
				{file: "2_c.js", identity: "id1", applied: 101},
			},
			sources: []testReconcileSource{{file: "3_b.js", identity: "id1"}},
		},
		{
			name: "rename target already occupied by another applied migration",
			history: []testHistorySeed{
				{file: "1_a.js", identity: "id1", applied: 100},
				{file: "2_b.js", identity: "id2", applied: 101},
			},
			sources: []testReconcileSource{{file: "2_b.js", identity: "id1"}},
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			app, _ := tests.NewTestApp()
			defer app.Cleanup()

			resetTestMigrationsHistory(t, app)
			seedTestMigrationsHistory(t, app, s.history...)

			before := allTestMigrationsHistory(t, app)

			runner := core.NewMigrationsRunner(app, newTestReconcileList(s.sources...))

			_, err := runner.ReconcileAppliedMigrations()
			if err == nil {
				t.Fatal("Expected reconciliation to fail due to undeterminable mapping, got nil")
			}
			if !errors.Is(err, core.ErrUndeterminableMigrationsHistory) {
				t.Fatalf("Expected ErrUndeterminableMigrationsHistory, got %v", err)
			}

			after := allTestMigrationsHistory(t, app)
			if len(after) != len(before) {
				t.Fatalf("Expected no history changes on error, got before=%+v after=%+v", before, after)
			}
			for i := range before {
				if after[i] != before[i] {
					t.Fatalf("Expected no history changes on error, got before=%+v after=%+v", before, after)
				}
			}
		})
	}
}

func TestMigrationsRunnerReconcileBeforeUp(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	resetTestMigrationsHistory(t, app)

	// the migration was applied under the old file name with the same content
	seedTestMigrationsHistory(t, app, testHistorySeed{file: "1_old_name.js", identity: "idRenamed", applied: 100})

	calls := []string{}

	list := core.MigrationsList{}
	// renamed source - should be skipped after the reconciliation renames the history entry
	list.Add(&core.Migration{
		File:     "1_new_name.js",
		Identity: "idRenamed",
		Up:       func(core.App) error { calls = append(calls, "renamed"); return nil },
	})
	// brand new migration - should be executed
	list.Add(&core.Migration{
		File:     "2_brand_new.js",
		Identity: "idNew",
		Up:       func(core.App) error { calls = append(calls, "new"); return nil },
	})

	runner := core.NewMigrationsRunner(app, list).WithHistoryReconciliation()

	applied, err := runner.Up()
	if err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	if len(applied) != 1 || applied[0] != "2_brand_new.js" {
		t.Fatalf("Expected only 2_brand_new.js to be applied, got %v", applied)
	}

	expectedCalls := `["new"]`
	callsJSON, err := json.Marshal(calls)
	if err != nil {
		t.Fatal(err)
	}
	if string(callsJSON) != expectedCalls {
		t.Fatalf("Expected Up calls %s, got %s", expectedCalls, callsJSON)
	}

	history := allTestMigrationsHistory(t, app)
	expectedHistory := []testHistoryRowState{
		{file: "1_new_name.js", identity: "idRenamed", applied: 100}, // renamed, applied time preserved
		{file: "2_brand_new.js", identity: "idNew"},                  // newly applied (applied value checked separately)
	}
	if len(history) != len(expectedHistory) {
		t.Fatalf("Expected %d history rows, got %d: %+v", len(expectedHistory), len(history), history)
	}
	for i, expected := range expectedHistory {
		if history[i].file != expected.file || history[i].identity != expected.identity {
			t.Fatalf("Expected history row %d to be %+v, got %+v", i, expected, history[i])
		}
	}
	if history[0].applied != 100 {
		t.Fatalf("Expected renamed row to preserve its applied time, got %d", history[0].applied)
	}
	// the old entry must be gone (replaced by the reconciled rename)
	if isMigrationApplied(app, "1_old_name.js") {
		t.Fatal("Expected the old migration name to be removed from history")
	}
	if !isMigrationApplied(app, "1_new_name.js") || !isMigrationApplied(app, "2_brand_new.js") {
		t.Fatalf("Expected both new file names to be marked as applied, got %+v", history)
	}

	// second run should not execute anything
	applied2, err := runner.Up()
	if err != nil {
		t.Fatalf("Second Up failed: %v", err)
	}
	if len(applied2) != 0 {
		t.Fatalf("Expected no migrations to be applied on the second run, got %v", applied2)
	}
	callsJSON2, _ := json.Marshal(calls)
	if string(callsJSON2) != expectedCalls {
		t.Fatalf("Expected no additional migration calls, got %s", callsJSON2)
	}
}

func TestMigrationsRunnerUpWithoutReconciliationDoesNotConverge(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	resetTestMigrationsHistory(t, app)

	seedTestMigrationsHistory(t, app, testHistorySeed{file: "1_old_name.js", identity: "idRenamed", applied: 100})

	calls := []string{}

	list := core.MigrationsList{}
	list.Add(&core.Migration{
		File:     "1_new_name.js",
		Identity: "idRenamed",
		Up:       func(core.App) error { calls = append(calls, "renamed"); return nil },
	})

	// runner without WithHistoryReconciliation keeps the old blunt behavior:
	// it doesn't touch history and treats the renamed file as new
	applied, err := core.NewMigrationsRunner(app, list).Up()
	if err != nil {
		t.Fatalf("Up failed: %v", err)
	}

	if len(applied) != 1 || applied[0] != "1_new_name.js" {
		t.Fatalf("Expected the renamed migration to be treated as new without reconciliation, got %v", applied)
	}

	if !isMigrationApplied(app, "1_old_name.js") {
		t.Fatal("Expected the old history entry to remain untouched without reconciliation enabled")
	}
}
