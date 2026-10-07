package core_test

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
)

// relationIntegrityTestFixtures prepares the following graph:
//
// c1 (reconcile target): records a, b
// c3 (other target): no records
// c2 with relation fields to c1 and c3:
//   - one:  single, no cascade, no required
//   - many: multi (maxSelect=10)
//   - cas:  single with CascadeDelete
//   - req:  single Required (no cascade)
//   - other: single -> c3 (not in c1 scope)
//
// c4 with a single CascadeDelete relation "rel" -> c2 (peer cascade)
//
// c2 records:
//
//	r1.one = b        (dangling after b is raw-deleted -> unset)
//	r2.many = [a, b]  (b dangling -> keep [a])
//	r3.cas = b        (dangling -> delete r3 and cascade-delete c4.q1)
//	r4.req = b        (dangling -> blocked, whole batch fails)
//	r5.other = z      (missing in c3 -> must NOT be touched by c1 reconcile)
//	r6.one = a        (valid -> must NOT be touched)
//
// c4 record: q1.rel = r3 (valid until r3 gets deleted, then peer-cascaded)
func relationIntegrityTestFixtures(t *testing.T, app core.App) (*core.Collection, *core.Collection) {
	t.Helper()

	c1 := core.NewBaseCollection("ri_c1")
	c1.Id = "ri_c1"
	c1.Fields.Add(&core.TextField{Name: "name"})
	if err := app.SaveNoValidate(c1); err != nil {
		t.Fatalf("failed to create c1: %v", err)
	}

	c3 := core.NewBaseCollection("ri_c3")
	c3.Id = "ri_c3"
	c3.Fields.Add(&core.TextField{Name: "name"})
	if err := app.SaveNoValidate(c3); err != nil {
		t.Fatalf("failed to create c3: %v", err)
	}

	c2 := core.NewBaseCollection("ri_c2")
	c2.Id = "ri_c2"
	c2.Fields.Add(
		&core.TextField{Name: "name"},
		&core.RelationField{Name: "one", CollectionId: c1.Id},
		&core.RelationField{Name: "many", CollectionId: c1.Id, MaxSelect: 10},
		&core.RelationField{Name: "cas", CollectionId: c1.Id, CascadeDelete: true},
		&core.RelationField{Name: "req", CollectionId: c1.Id, Required: true},
		&core.RelationField{Name: "other", CollectionId: c3.Id},
	)
	if err := app.SaveNoValidate(c2); err != nil {
		t.Fatalf("failed to create c2: %v", err)
	}

	c4 := core.NewBaseCollection("ri_c4")
	c4.Id = "ri_c4"
	c4.Fields.Add(
		&core.RelationField{Name: "rel", CollectionId: c2.Id, CascadeDelete: true},
	)
	if err := app.SaveNoValidate(c4); err != nil {
		t.Fatalf("failed to create c4: %v", err)
	}

	mustCreate := func(c *core.Collection, id string, values map[string]any) {
		t.Helper()
		r := core.NewRecord(c)
		r.Id = id
		for k, v := range values {
			r.Set(k, v)
		}
		if err := app.SaveNoValidate(r); err != nil {
			t.Fatalf("failed to create %s: %v", id, err)
		}
	}

	mustCreate(c1, "a", map[string]any{"name": "a"})
	mustCreate(c1, "b", map[string]any{"name": "b"})

	mustCreate(c2, "r1", map[string]any{"one": "b"})
	mustCreate(c2, "r2", map[string]any{"many": []string{"a", "b"}})
	mustCreate(c2, "r3", map[string]any{"cas": "b"})
	mustCreate(c2, "r4", map[string]any{"req": "b"})
	mustCreate(c2, "r5", map[string]any{"other": "z"})
	mustCreate(c2, "r6", map[string]any{"one": "a"})

	mustCreate(c4, "q1", map[string]any{"rel": "r3"})

	// simulate manual deletion that bypasses the cascade rules
	_, err := app.NonconcurrentDB().
		NewQuery("DELETE FROM {{ri_c1}} WHERE [[id]]={:id}").
		Bind(dbx.Params{"id": "b"}).
		Execute()
	if err != nil {
		t.Fatalf("failed to raw-delete b: %v", err)
	}

	return c1, c3
}

func TestReconcileRelationIntegrityBlockedIsAtomic(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	c1, _ := relationIntegrityTestFixtures(t, app)

	report, err := app.ReconcileRelationIntegrity(c1)
	if err == nil {
		t.Fatal("expected blocked error, got nil")
	}

	blockedErr, ok := err.(*core.RelationIntegrityBlockedError)
	if !ok {
		t.Fatalf("expected RelationIntegrityBlockedError, got %T: %v", err, err)
	}
	if len(blockedErr.Issues) != 1 || blockedErr.Issues[0].RecordId != "r4" || blockedErr.Issues[0].FieldName != "req" {
		t.Fatalf("unexpected blocked issues: %+v", blockedErr.Issues)
	}

	// nothing must have been applied
	if report.InvalidReferences != 4 {
		t.Fatalf("expected 4 invalid references, got %d", report.InvalidReferences)
	}
	if report.CheckedReferences != 6 {
		t.Fatalf("expected 6 checked references, got %d", report.CheckedReferences)
	}
	if len(report.UpdatedRecordIds) != 0 || len(report.DeletedRecordIds) != 0 {
		t.Fatalf("expected no applied changes, got updated=%v deleted=%v", report.UpdatedRecordIds, report.DeletedRecordIds)
	}

	r1, _ := app.FindRecordById("ri_c2", "r1")
	if got := r1.GetString("one"); got != "b" {
		t.Fatalf("expected r1.one to remain b, got %q", got)
	}
	r2, _ := app.FindRecordById("ri_c2", "r2")
	if got := r2.GetStringSlice("many"); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("expected r2.many to remain [a b], got %v", got)
	}
	if r3, _ := app.FindRecordById("ri_c2", "r3"); r3 == nil {
		t.Fatal("expected r3 to still exist")
	}
	if q1, _ := app.FindRecordById("ri_c4", "q1"); q1 == nil {
		t.Fatal("expected q1 to still exist")
	}
	r5, _ := app.FindRecordById("ri_c2", "r5")
	if got := r5.GetString("other"); got != "z" {
		t.Fatalf("expected r5.other to remain z, got %q", got)
	}

	// the conclusions must be durably staged in the ledger
	staged, err := app.FindRelationIntegrityIssues(c1.Id, "staged")
	if err != nil {
		t.Fatal(err)
	}
	if len(staged) != 4 {
		t.Fatalf("expected 4 staged ledger rows, got %d", len(staged))
	}
	actions := map[string]string{}
	for _, issue := range staged {
		actions[issue.RecordId+"/"+issue.FieldName] = issue.Action
	}
	expectedActions := map[string]string{
		"r1/one":  "unset",
		"r2/many": "unset",
		"r3/cas":  "cascade_delete",
		"r4/req":  "blocked",
	}
	for k, expected := range expectedActions {
		if actions[k] != expected {
			t.Fatalf("expected %s action %q, got %q (all: %v)", k, expected, actions[k], actions)
		}
	}
}

func TestReconcileRelationIntegrityApplyAndConsistency(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	c1, c3 := relationIntegrityTestFixtures(t, app)

	// first run is blocked
	if _, err := app.ReconcileRelationIntegrity(c1); err == nil {
		t.Fatal("expected first run to be blocked")
	}

	// resolve the blocking reference manually
	r4, err := app.FindRecordById("ri_c2", "r4")
	if err != nil {
		t.Fatal(err)
	}
	r4.Set("req", "a")
	if err := app.SaveNoValidate(r4); err != nil {
		t.Fatal(err)
	}

	// valid record that must not be touched
	r6Before, err := app.FindRecordById("ri_c2", "r6")
	if err != nil {
		t.Fatal(err)
	}
	r6UpdatedBefore := r6Before.GetString("updated")

	// rerun: applies the whole batch atomically and converges
	report, err := app.ReconcileRelationIntegrity(c1)
	if err != nil {
		t.Fatalf("expected successful reconcile, got %v", err)
	}

	if report.InvalidReferences != 3 {
		t.Fatalf("expected 3 invalid references, got %d", report.InvalidReferences)
	}
	if !slices.Equal(report.UpdatedRecordIds, []string{"r1", "r2"}) {
		t.Fatalf("expected r1,r2 updates, got %v", report.UpdatedRecordIds)
	}
	if !slices.Equal(report.DeletedRecordIds, []string{"r3"}) {
		t.Fatalf("expected r3 deletion, got %v", report.DeletedRecordIds)
	}

	// single dangling ref unset
	r1, _ := app.FindRecordById("ri_c2", "r1")
	if got := r1.GetString("one"); got != "" {
		t.Fatalf("expected r1.one to be unset, got %q", got)
	}

	// multi dangling ref stripped, valid kept
	r2, _ := app.FindRecordById("ri_c2", "r2")
	if got := r2.GetStringSlice("many"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("expected r2.many [a], got %v", got)
	}

	// cascade record gone together with its peer (c4.q1 -> r3 cascade)
	if r3, _ := app.FindRecordById("ri_c2", "r3"); r3 != nil {
		t.Fatal("expected r3 to be deleted")
	}
	if q1, _ := app.FindRecordById("ri_c4", "q1"); q1 != nil {
		t.Fatal("expected q1 to be cascade-deleted together with r3")
	}

	// fixed record preserved
	r4After, _ := app.FindRecordById("ri_c2", "r4")
	if got := r4After.GetString("req"); got != "a" {
		t.Fatalf("expected r4.req a, got %q", got)
	}

	// other collections' references untouched
	r5, _ := app.FindRecordById("ri_c2", "r5")
	if got := r5.GetString("other"); got != "z" {
		t.Fatalf("expected r5.other (ri_c3 scope) to remain z, got %q", got)
	}

	// valid reference untouched (no resave -> updated unchanged)
	r6After, _ := app.FindRecordById("ri_c2", "r6")
	if got := r6After.GetString("updated"); got != r6UpdatedBefore {
		t.Fatalf("expected r6 to be untouched, updated changed %q -> %q", r6UpdatedBefore, got)
	}
	if got := r6After.GetString("one"); got != "a" {
		t.Fatalf("expected r6.one a, got %q", got)
	}

	// ledger state: everything resolved, nothing pending
	staged, err := app.FindRelationIntegrityIssues(c1.Id, "staged")
	if err != nil {
		t.Fatal(err)
	}
	if len(staged) != 0 {
		t.Fatalf("expected no staged rows, got %d", len(staged))
	}
	resolved, err := app.FindRelationIntegrityIssues(c1.Id, "resolved")
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 3 {
		t.Fatalf("expected 3 resolved rows, got %d", len(resolved))
	}

	// reads and reference-based filters see consistent results
	if _, err := app.FindFirstRecordByFilter("ri_c2", "one='b'"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no record matching the dangling one='b', got %v", err)
	}
	if rec, err := app.FindFirstRecordByFilter("ri_c2", "many:each='a'"); err != nil || rec.Id != "r2" {
		t.Fatalf("expected r2 via many:each='a' filter, got %v %v", rec, err)
	}
	matches, err := app.FindRecordsByFilter("ri_c4", "rel='r3'", "", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("expected no c4 records matching dangling rel='r3', got %d", len(matches))
	}
	if rec, err := app.FindFirstRecordByFilter("ri_c2", "req='a'"); err != nil || rec.Id != "r4" {
		t.Fatalf("expected r4 via req='a', got %v %v", rec, err)
	}

	// rerun after convergence is a no-op
	repeatReport, err := app.ReconcileRelationIntegrity(c1)
	if err != nil {
		t.Fatalf("expected no error on rerun, got %v", err)
	}
	if repeatReport.InvalidReferences != 0 || len(repeatReport.UpdatedRecordIds) != 0 || len(repeatReport.DeletedRecordIds) != 0 {
		t.Fatalf("expected converged no-op rerun, got %+v", repeatReport)
	}
	if repeatReport.CheckedReferences != 3 {
		t.Fatalf("expected 3 checked refs after fix (a,a,a), got %d", repeatReport.CheckedReferences)
	}

	// now reconcile the other target collection - its dangling ref gets fixed
	otherReport, err := app.ReconcileRelationIntegrity(c3)
	if err != nil {
		t.Fatalf("expected c3 reconcile success, got %v", err)
	}
	if otherReport.InvalidReferences != 1 || !slices.Equal(otherReport.UpdatedRecordIds, []string{"r5"}) {
		t.Fatalf("unexpected c3 report: %+v", otherReport)
	}
	r5After, _ := app.FindRecordById("ri_c2", "r5")
	if got := r5After.GetString("other"); got != "" {
		t.Fatalf("expected r5.other to be unset, got %q", got)
	}
}

func TestReconcileRelationIntegrityRerunAfterCrashConverges(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	c1, c3 := relationIntegrityTestFixtures(t, app)

	// blocked run first (r4) - creates the ledger table, data stays untouched
	if _, err := app.ReconcileRelationIntegrity(c1); err == nil {
		t.Fatal("expected blocked run")
	}

	// simulate a stale staged row left over from a crashed run, pointing to a
	// record/field/ref that is no longer invalid
	_, err := app.NonconcurrentDB().NewQuery(fmt.Sprintf(`
INSERT INTO {{%s}} (
  [[id]], [[collectionId]], [[collectionName]], [[recordId]], [[fieldId]], [[fieldName]],
  [[targetCollectionId]], [[targetCollectionName]], [[refId]], [[action]], [[reason]],
  [[status]], [[created]], [[updated]]
) VALUES (
  'stale-row', 'ri_c2', 'ri_c2', 'r1', 'whatever', 'one',
  {:target}, 'ri_c1', 'b', 'unset', 'missing_relation_record',
  'staged', '2020-01-01T00:00:00Z', '2020-01-01T00:00:00Z'
)`, "_relationIntegrityIssues")).
		Bind(dbx.Params{"target": c1.Id}).
		Execute()
	if err != nil {
		t.Fatalf("failed to insert stale staged row: %v", err)
	}

	// fix blocker and rerun - staged state must be rebuilt deterministically
	r4, _ := app.FindRecordById("ri_c2", "r4")
	r4.Set("req", "a")
	if err := app.SaveNoValidate(r4); err != nil {
		t.Fatal(err)
	}

	if _, err := app.ReconcileRelationIntegrity(c1); err != nil {
		t.Fatalf("expected successful rerun, got %v", err)
	}

	// the fabricated stale row must have converged away
	var staleCount int
	err = app.DB().
		Select("count(*)").
		From("_relationIntegrityIssues").
		AndWhere(dbx.HashExp{"id": "stale-row"}).
		Row(&staleCount)
	if err != nil {
		t.Fatal(err)
	}
	if staleCount != 0 {
		t.Fatalf("expected stale staged row to be removed on rerun, found %d", staleCount)
	}

	staged, err := app.FindRelationIntegrityIssues(c1.Id, "staged")
	if err != nil {
		t.Fatal(err)
	}
	if len(staged) != 0 {
		t.Fatalf("expected no pending work, got %d staged", len(staged))
	}

	// reintroduce the same dangling ref after it was resolved: the ledger row
	// flips back to staged and then resolved again and the data converges.
	_, err = app.NonconcurrentDB().
		NewQuery("UPDATE {{ri_c2}} SET [[other]]={:v} WHERE [[id]]='r5'").
		Bind(dbx.Params{"v": "z"}).
		Execute()
	if err != nil {
		t.Fatal(err)
	}

	report, err := app.ReconcileRelationIntegrity(c3)
	if err != nil {
		t.Fatalf("expected c3 reconcile success, got %v", err)
	}
	if report.InvalidReferences != 1 {
		t.Fatalf("expected 1 invalid c3 ref, got %d", report.InvalidReferences)
	}

	r5, _ := app.FindRecordById("ri_c2", "r5")
	if got := r5.GetString("other"); got != "" {
		t.Fatalf("expected r5.other converged to empty, got %q", got)
	}

	c3Staged, err := app.FindRelationIntegrityIssues(c3.Id, "staged")
	if err != nil {
		t.Fatal(err)
	}
	if len(c3Staged) != 0 {
		t.Fatalf("expected no c3 staged rows after converge, got %d", len(c3Staged))
	}
}

func TestReconcileRelationIntegrityCascadeOverridesBlockedAcrossFields(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	x1 := core.NewBaseCollection("ri_x1")
	x1.Id = "ri_x1"
	if err := app.SaveNoValidate(x1); err != nil {
		t.Fatal(err)
	}

	x2 := core.NewBaseCollection("ri_x2")
	x2.Id = "ri_x2"
	x2.Fields.Add(
		&core.RelationField{Name: "fc", CollectionId: x1.Id, CascadeDelete: true},
		&core.RelationField{Name: "fr", CollectionId: x1.Id, Required: true},
	)
	if err := app.SaveNoValidate(x2); err != nil {
		t.Fatal(err)
	}

	gone := core.NewRecord(x1)
	gone.Id = "gone"
	if err := app.SaveNoValidate(gone); err != nil {
		t.Fatal(err)
	}

	s1 := core.NewRecord(x2)
	s1.Id = "s1"
	s1.Set("fc", "gone") // would cascade-delete s1
	s1.Set("fr", "gone") // would be blocked on its own
	if err := app.SaveNoValidate(s1); err != nil {
		t.Fatal(err)
	}

	// bypass cascade
	if _, err := app.NonconcurrentDB().
		NewQuery("DELETE FROM {{ri_x1}} WHERE [[id]]={:id}").
		Bind(dbx.Params{"id": "gone"}).
		Execute(); err != nil {
		t.Fatal(err)
	}

	report, err := app.ReconcileRelationIntegrity(x1)
	if err != nil {
		t.Fatalf("expected cascade to override blocked, got %v", err)
	}
	if !slices.Equal(report.DeletedRecordIds, []string{"s1"}) {
		t.Fatalf("expected s1 to be deleted, got %+v", report)
	}

	if rec, _ := app.FindRecordById(x2, "s1"); rec != nil {
		t.Fatal("expected s1 to be gone")
	}

	// both ledger rows must carry the cascade_delete verdict (not blocked)
	issues, err := app.FindRelationIntegrityIssues(x1.Id)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 2 {
		t.Fatalf("expected 2 ledger rows, got %d", len(issues))
	}
	for _, issue := range issues {
		if issue.Action != "cascade_delete" {
			t.Fatalf("expected cascade_delete action for %s, got %s", issue.FieldName, issue.Action)
		}
	}
}

func TestReconcileRelationIntegrityValidation(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	if _, err := app.ReconcileRelationIntegrity(nil); err == nil {
		t.Fatal("expected error for nil collection")
	}

	view := core.NewViewCollection("ri_view")
	if _, err := app.ReconcileRelationIntegrity(view); err == nil {
		t.Fatal("expected error for view collection")
	}
}
