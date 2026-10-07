package core

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/pocketbase/dbx"
	validation "github.com/pocketbase/ozzo-validation/v4"
	"github.com/pocketbase/pocketbase/tools/dbutils"
	"github.com/pocketbase/pocketbase/tools/security"
)

const (
	reconcileBackupTablePrefix = "__pb_reconcile_backup_"
	reconcileShadowTablePrefix = "__pb_reconcile_new_"
)

// ReconcileCollectionReport summarizes the changes applied on a single
// collection record table.
type ReconcileCollectionReport struct {
	Collection *Collection

	// SkippedReason is populated when the collection wasn't reconciled
	// (eg. because it is a view and doesn't have a record table).
	SkippedReason string

	// CreatedTable is true when the record table was missing and got created.
	CreatedTable bool

	// AddedColumns lists the columns that were missing from the record table and got added.
	AddedColumns []string

	// DroppedColumns lists the extra columns that were removed from the record table.
	//
	// Before the removal the raw column values are archived into the backup table
	// (see [ReconcileCollectionReport.BackupTableName]) to ensure that no data is lost.
	DroppedColumns []string

	// AlteredColumns lists existing columns whose DB definition
	// (type/default/not null) didn't match and got repaired.
	//
	// The original raw column values are archived into the backup table first.
	AlteredColumns []string

	// CreatedIndexes lists the names of the created indexes.
	CreatedIndexes []string

	// DroppedIndexes lists the names of the dropped indexes.
	DroppedIndexes []string
}

// BackupTableName returns the name of the table where the raw values of
// removed or altered columns are archived.
func (r *ReconcileCollectionReport) BackupTableName() string {
	return reconcileBackupTablePrefix + r.Collection.Name
}

// HasChanges checks whether the reconcile run applied any schema change.
func (r *ReconcileCollectionReport) HasChanges() bool {
	return r.hasChanges()
}

func (r *ReconcileCollectionReport) hasChanges() bool {
	return r.CreatedTable ||
		len(r.AddedColumns) > 0 ||
		len(r.DroppedColumns) > 0 ||
		len(r.AlteredColumns) > 0 ||
		len(r.CreatedIndexes) > 0 ||
		len(r.DroppedIndexes) > 0
}

// ReconcileReport summarizes the result of a bulk record tables reconcile.
type ReconcileReport struct {
	Collections []*ReconcileCollectionReport
}

// HasChanges checks whether the reconcile run applied any schema change.
func (r *ReconcileReport) HasChanges() bool {
	for _, c := range r.Collections {
		if c.hasChanges() {
			return true
		}
	}
	return false
}

// ReconcileRecordTable compares the physical record table of the specified
// non-view collection with its definition and converges it to the expected
// schema:
//   - creates the record table if it is missing;
//   - adds missing columns;
//   - removes extra columns (their raw values are archived first);
//   - repairs column type/default/not null mismatches (raw values archived first);
//   - drops and (re)creates indexes to match the collection definition.
//
// The operation is fully idempotent - running it on an already converged
// table is a no-op. Each collection is reconciled in its own transaction,
// so if the process gets interrupted, rerunning it continues from the
// already converged collections.
func (app *BaseApp) ReconcileRecordTable(collection *Collection) (*ReconcileCollectionReport, error) {
	report := &ReconcileCollectionReport{Collection: collection}

	if collection.IsView() {
		report.SkippedReason = "view collection"
		return report, nil
	}

	txErr := app.RunInTransaction(func(txApp App) error {
		return reconcileRecordTable(txApp, report)
	})
	if txErr != nil {
		return report, txErr
	}

	if report.hasChanges() {
		if _, err := app.NonconcurrentDB().NewQuery("PRAGMA optimize").Execute(); err != nil {
			app.Logger().Warn(
				"Failed to run PRAGMA optimize after record table reconcile",
				slog.String("error", err.Error()),
			)
		}
	}

	return report, nil
}

// ReconcileRecordTables reconciles all non-view collection record tables.
//
// If collections are explicitly provided, only them are processed; otherwise
// all collections from the database are used.
//
// Tables that don't belong to a collection are never touched.
//
// Each collection is processed in its own transaction and a failure doesn't
// stop the remaining collections (all errors are combined). On rerun the
// already converged collections are skipped and failed ones are retried.
func (app *BaseApp) ReconcileRecordTables(optCollections ...*Collection) (*ReconcileReport, error) {
	collections := optCollections

	if len(collections) == 0 {
		var err error
		collections, err = app.FindAllCollections()
		if err != nil {
			return nil, err
		}
	}

	result := &ReconcileReport{}
	var errs []error

	for _, collection := range collections {
		report, err := app.ReconcileRecordTable(collection)
		result.Collections = append(result.Collections, report)
		if err != nil {
			errs = append(errs, fmt.Errorf("[%s] %w", collection.Name, err))
		}
	}

	return result, errors.Join(errs...)
}

func reconcileRecordTable(app App, report *ReconcileCollectionReport) error {
	collection := report.Collection
	tableName := collection.Name

	// create the record table if it is entirely missing
	if !app.HasTable(tableName) {
		return reconcileMissingRecordTable(app, report)
	}

	// inspect the actual physical columns
	info, err := app.TableInfo(tableName)
	if err != nil {
		return err
	}

	actualColumns := map[string]*TableInfoRow{}
	for _, row := range info {
		actualColumns[row.Name] = row
	}

	// parse the expected field columns
	var expected []expectedColumn
	expectedLowerNames := map[string]bool{}
	for _, field := range collection.Fields {
		raw := field.ColumnType(app)
		expected = append(expected, expectedColumn{
			raw: raw,
			def: parseColumnDefinition(field.GetName(), raw),
		})
		expectedLowerNames[strings.ToLower(field.GetName())] = true
	}

	var missing []expectedColumn
	var mismatched []expectedColumn
	for _, item := range expected {
		actual := findTableInfoRow(actualColumns, item.def.Name)
		switch {
		case actual == nil:
			missing = append(missing, item)
		case !item.def.matchesActual(actual):
			mismatched = append(mismatched, item)
		}
	}

	var extra []string
	for name := range actualColumns {
		if !expectedLowerNames[strings.ToLower(name)] {
			extra = append(extra, name)
		}
	}
	slices.Sort(extra)

	// plan the index changes (this also validates the declared indexes
	// before applying any mutation).
	//
	// When columns are added/removed/redefined, all indexes are recreated,
	// matching the regular collection update flow.
	columnDrift := len(missing)+len(mismatched)+len(extra) > 0
	indexes, err := planCollectionIndexes(app, collection, columnDrift)
	if err != nil {
		return err
	}

	// SQLite cannot ADD COLUMN with PRIMARY KEY or non-constant default,
	// so such missing columns require a full table rebuild. A primary
	// column mismatch (eg. the id lost its PK) also can't be fixed incrementally.
	needRebuild := false
	for _, item := range missing {
		if item.def.Primary || (item.def.HasDefault && strings.Contains(item.def.Default, "(")) {
			needRebuild = true
			break
		}
	}
	if !needRebuild {
		for _, item := range mismatched {
			if item.def.Primary {
				needRebuild = true
				break
			}
		}
	}

	// already converged - nothing to do
	if len(missing) == 0 && len(mismatched) == 0 && len(extra) == 0 &&
		len(indexes.drop) == 0 && len(indexes.create) == 0 {
		return nil
	}

	// 1. drop indexes to prevent conflicts with the column changes
	if err := execIndexDrops(app, indexes, report); err != nil {
		return err
	}

	// 2. archive the raw values of the extra and mismatched columns
	if err := archiveColumns(app, collection, buildArchiveColumns(actualColumns, extra, mismatched)); err != nil {
		return err
	}

	// 3. try to drop the extra columns incrementally
	failedDrop := ""
	for _, name := range extra {
		if _, err := app.DB().DropColumn(tableName, name).Execute(); err != nil {
			// eg. the column is used by a trigger; a full rebuild will handle it
			failedDrop = name
			break
		}
		report.DroppedColumns = append(report.DroppedColumns, name)
	}

	// 4. repair the remaining column drift
	if needRebuild || failedDrop != "" {
		if failedDrop != "" {
			if i := slices.Index(extra, failedDrop); i >= 0 {
				report.DroppedColumns = append(report.DroppedColumns, extra[i:]...)
			}
		}

		if err := rebuildRecordTable(app, collection); err != nil {
			return err
		}

		for _, item := range missing {
			report.AddedColumns = append(report.AddedColumns, item.def.Name)
		}
		for _, item := range mismatched {
			report.AlteredColumns = append(report.AlteredColumns, item.def.Name)
		}
	} else {
		for _, item := range missing {
			if _, err := app.DB().AddColumn(tableName, item.def.Name, item.raw).Execute(); err != nil {
				return fmt.Errorf("failed to add missing column %s: %w", item.def.Name, err)
			}
			report.AddedColumns = append(report.AddedColumns, item.def.Name)
		}

		if len(mismatched) > 0 {
			if err := replaceMismatchedColumns(app, collection, mismatched); err != nil {
				return err
			}
			for _, item := range mismatched {
				report.AlteredColumns = append(report.AlteredColumns, item.def.Name)
			}
		}
	}

	// 5. (re)create the declared indexes
	return execIndexCreates(app, indexes, report)
}

func reconcileMissingRecordTable(app App, report *ReconcileCollectionReport) error {
	collection := report.Collection

	cols := make(map[string]string, len(collection.Fields))
	for _, field := range collection.Fields {
		cols[field.GetName()] = field.ColumnType(app)
	}

	if _, err := app.DB().CreateTable(collection.Name, cols).Execute(); err != nil {
		return err
	}
	report.CreatedTable = true

	plan, err := planCollectionIndexes(app, collection, false)
	if err != nil {
		return err
	}

	return execIndexCreates(app, plan, report)
}

// -------------------------------------------------------------------
// Column definition parsing and comparison
// -------------------------------------------------------------------

type expectedColumn struct {
	raw string
	def columnDefinition
}

type columnDefinition struct {
	Name       string
	Type       string
	NotNull    bool
	Primary    bool
	HasDefault bool
	Default    string
}

var columnDefTypeRegex = regexp.MustCompile(`(?is)^\s*([a-z_]\w*)\s*([\s\S]*)$`)

func parseColumnDefinition(name, raw string) columnDefinition {
	def := columnDefinition{Name: name}

	matches := columnDefTypeRegex.FindStringSubmatch(strings.TrimSpace(raw))
	if matches == nil {
		return def
	}

	def.Type = matches[1]

	// normalize whitespace for keyword/default extraction
	rest := strings.Join(strings.Fields(matches[2]), " ")
	upper := strings.ToUpper(rest)

	def.NotNull = strings.Contains(upper, "NOT NULL")
	def.Primary = strings.Contains(upper, "PRIMARY KEY")

	if i := strings.Index(upper, "DEFAULT"); i >= 0 {
		expr := strings.TrimSpace(rest[i+len("DEFAULT"):])
		if strings.HasSuffix(strings.ToUpper(expr), "NOT NULL") {
			expr = strings.TrimSpace(expr[:len(expr)-len("NOT NULL")])
		}
		def.HasDefault = true
		def.Default = expr
	}

	return def
}

func normalizeExpr(str string) string {
	s := strings.Join(strings.Fields(strings.TrimSpace(str)), " ")

	// strip redundant outer parentheses (SQLite stores eg.
	// `('r'||lower(...))` without the outer parens)
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' && outerParenWrapsAll(s) {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}

	return s
}

// outerParenWrapsAll reports whether the opening parenthesis is matched only
// by the final closing one (ie. the whole expression is wrapped in it).
func outerParenWrapsAll(s string) bool {
	depth := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && i != len(s)-1 {
			return false
		}
	}
	return true
}

// matchesActual compares the expected definition with an actual PRAGMA
// table_info row.
func (def columnDefinition) matchesActual(actual *TableInfoRow) bool {
	primaryActual := actual.PK != 0

	// Legacy-compatible primary column: the id non-constant default and NOT
	// NULL flags were introduced later as raw-SQL insertion guards and old
	// tables were never retrofitted (PocketBase itself doesn't migrate them).
	//
	// Accept such legacy declarations as compatible (same type + PK; default
	// either missing or equivalent). Any other primary column drift requires
	// a table rebuild and is handled by the caller.
	if def.Primary && def.HasDefault && strings.Contains(def.Default, "(") {
		return strings.EqualFold(def.Type, actual.Type) &&
			primaryActual &&
			(!actual.DefaultValue.Valid ||
				strings.EqualFold(normalizeExpr(def.Default), normalizeExpr(actual.DefaultValue.String)))
	}

	if !strings.EqualFold(def.Type, actual.Type) {
		return false
	}

	if def.NotNull != actual.NotNull {
		return false
	}

	if def.Primary != primaryActual {
		return false
	}

	switch {
	case !def.HasDefault:
		return !actual.DefaultValue.Valid
	case strings.EqualFold(def.Default, "NULL"):
		// note: a column declared "DEFAULT NULL" may be returned by PRAGMA
		// as a valid default with value "NULL"
		return !actual.DefaultValue.Valid ||
			strings.EqualFold(strings.TrimSpace(actual.DefaultValue.String), "NULL")
	default:
		return actual.DefaultValue.Valid &&
			strings.EqualFold(normalizeExpr(def.Default), normalizeExpr(actual.DefaultValue.String))
	}
}

// -------------------------------------------------------------------
// Values archiving
// -------------------------------------------------------------------

func buildArchiveColumns(
	actualColumns map[string]*TableInfoRow,
	extra []string,
	mismatched []expectedColumn,
) map[string]string {
	result := map[string]string{}

	for _, name := range extra {
		typ := "TEXT"
		if row := findTableInfoRow(actualColumns, name); row != nil && row.Type != "" {
			typ = row.Type
		}
		result[name] = typ
	}

	for _, item := range mismatched {
		result[item.def.Name] = item.def.Type
	}

	return result
}

// archiveColumns stores the raw values of the specified live columns into a
// dedicated backup table before they get removed or redefined.
//
// The backup is append-only and keyed by the record id:
//   - the table and its columns are created only if missing;
//   - already archived values (including from previous interrupted runs)
//     are preserved and never overwritten.
func archiveColumns(app App, collection *Collection, columns map[string]string) error {
	if len(columns) == 0 {
		return nil
	}

	backupName := reconcileBackupTablePrefix + collection.Name
	tableName := collection.Name

	if !app.HasTable(backupName) {
		query := fmt.Sprintf("CREATE TABLE {{%s}} ([[id]] TEXT PRIMARY KEY)", backupName)
		if _, err := app.DB().NewQuery(query).Execute(); err != nil {
			return fmt.Errorf("failed to create reconcile backup table: %w", err)
		}
	}

	backupInfo, err := app.TableInfo(backupName)
	if err != nil {
		return err
	}

	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	slices.Sort(names)

	// add the backup columns that don't exist yet
	for _, name := range names {
		if findTableInfoRowByName(backupInfo, name) != nil {
			continue // archived in a previous run
		}

		typ := strings.TrimSpace(columns[name])
		if typ == "" {
			typ = "TEXT"
		}

		query := fmt.Sprintf("ALTER TABLE {{%s}} ADD COLUMN [[%s]] %s", backupName, name, typ)
		if _, err := app.DB().NewQuery(query).Execute(); err != nil {
			return fmt.Errorf("failed to add backup column %s: %w", name, err)
		}
	}

	// use rowid as identity when the live table doesn't have an id column
	liveInfo, err := app.TableInfo(tableName)
	if err != nil {
		return err
	}

	idSource := "rowid"
	if findTableInfoRowByName(liveInfo, FieldNameId) != nil {
		idSource = FieldNameId
	}

	// ensure one backup row per live record
	insertQuery := fmt.Sprintf(
		"INSERT OR IGNORE INTO {{%s}} ([[id]]) SELECT CAST([[%s]] AS TEXT) FROM {{%s}}",
		backupName, idSource, tableName,
	)
	if _, err := app.DB().NewQuery(insertQuery).Execute(); err != nil {
		return fmt.Errorf("failed to populate reconcile backup ids: %w", err)
	}

	// fill the archived values (first archived non-null value wins)
	for _, name := range names {
		updateQuery := fmt.Sprintf(
			"UPDATE {{%s}} SET [[%s]] = ("+
				"SELECT c.[[%s]] FROM {{%s}} c WHERE CAST(c.[[%s]] AS TEXT) = {{%s}}.[[id]]"+
				") WHERE {{%s}}.[[%s]] IS NULL",
			backupName, name,
			name, tableName, idSource, backupName,
			backupName, name,
		)
		if _, err := app.DB().NewQuery(updateQuery).Execute(); err != nil {
			return fmt.Errorf("failed to archive column %s: %w", name, err)
		}
	}

	return nil
}

// -------------------------------------------------------------------
// Column repairs
// -------------------------------------------------------------------

// replaceMismatchedColumns repairs the provided columns one by one using the
// temp-rename -> add -> copy -> drop-temp pattern, preserving all data and
// applying the new column type affinity/default.
func replaceMismatchedColumns(app App, collection *Collection, items []expectedColumn) error {
	tableName := collection.Name

	// Temporarily drop all views because renaming a column referenced by a
	// view raises an error; the views are restored afterwards.
	views := []struct {
		Name string `db:"name"`
		SQL  string `db:"sql"`
	}{}
	err := app.DB().Select("name", "sql").
		From("sqlite_master").
		AndWhere(dbx.NewExp("sql is not null")).
		AndWhere(dbx.HashExp{"type": "view"}).
		All(&views)
	if err != nil {
		return err
	}
	for _, view := range views {
		if err := app.DeleteView(view.Name); err != nil {
			return err
		}
	}

	for _, item := range items {
		name := item.def.Name
		tempName := "_" + name + security.PseudorandomString(5)

		if _, err := app.DB().RenameColumn(tableName, name, tempName).Execute(); err != nil {
			return fmt.Errorf("failed to rename mismatched column %s: %w", name, err)
		}

		if _, err := app.DB().AddColumn(tableName, name, item.raw).Execute(); err != nil {
			return fmt.Errorf("failed to re-add column %s: %w", name, err)
		}

		// value used when the old value is NULL but the new column is NOT NULL
		fallback := "''"
		if item.def.HasDefault {
			switch d := item.def.Default; {
			case strings.EqualFold(d, "NULL"):
				fallback = "NULL"
			case !strings.Contains(d, "("):
				fallback = d
			}
		}

		copyQuery := fmt.Sprintf(
			"UPDATE {{%s}} SET [[%s]] = COALESCE([[%s]], %s)",
			tableName, name, tempName, fallback,
		)
		if _, err := app.DB().NewQuery(copyQuery).Execute(); err != nil {
			return fmt.Errorf("failed to normalize values of column %s: %w", name, err)
		}

		if _, err := app.DB().DropColumn(tableName, tempName).Execute(); err != nil {
			return fmt.Errorf("failed to drop temp column %s: %w", tempName, err)
		}
	}

	// restore the views
	for _, view := range views {
		if _, err := app.DB().NewQuery(view.SQL).Execute(); err != nil {
			return err
		}
	}

	return nil
}

// rebuildRecordTable performs the SQLite "12-step" table rebuild:
// it creates a shadow table with the exact collection column definitions,
// copies all data (with row count verification), drops the old table and
// renames the shadow in its place.
func rebuildRecordTable(app App, collection *Collection) error {
	tableName := collection.Name
	shadowName := reconcileShadowTablePrefix + security.PseudorandomString(8)

	cols := make(map[string]string, len(collection.Fields))
	for _, field := range collection.Fields {
		cols[field.GetName()] = field.ColumnType(app)
	}

	if _, err := app.DB().CreateTable(shadowName, cols).Execute(); err != nil {
		return fmt.Errorf("failed to create shadow record table: %w", err)
	}

	liveInfo, err := app.TableInfo(tableName)
	if err != nil {
		return err
	}

	// only copy columns that exist in both tables; missing ones get their defaults
	var shared []string
	for _, field := range collection.Fields {
		if findTableInfoRowByName(liveInfo, field.GetName()) != nil {
			shared = append(shared, field.GetName())
		}
	}

	if len(shared) > 0 {
		quoted := "[[" + strings.Join(shared, "]],[[") + "]]"
		insertQuery := fmt.Sprintf(
			"INSERT INTO {{%s}} (%s) SELECT %s FROM {{%s}}",
			shadowName, quoted, quoted, tableName,
		)
		if _, err := app.DB().NewQuery(insertQuery).Execute(); err != nil {
			return fmt.Errorf("failed to copy records into the shadow table: %w", err)
		}
	}

	var oldCount, newCount int64
	if err := app.DB().Select("count(*)").From(tableName).Row(&oldCount); err != nil {
		return err
	}
	if err := app.DB().Select("count(*)").From(shadowName).Row(&newCount); err != nil {
		return err
	}
	if oldCount != newCount {
		return fmt.Errorf("shadow table row count mismatch: expected %d, got %d", oldCount, newCount)
	}

	if _, err := app.DB().DropTable(tableName).Execute(); err != nil {
		return fmt.Errorf("failed to drop the old record table: %w", err)
	}

	if _, err := app.DB().RenameTable(shadowName, tableName).Execute(); err != nil {
		return fmt.Errorf("failed to restore the record table name: %w", err)
	}

	return nil
}

// -------------------------------------------------------------------
// Index reconciliation
// -------------------------------------------------------------------

type indexPlan struct {
	drop   []string
	create []dbutils.Index
}

// planCollectionIndexes compares the actual table indexes with the ones
// declared in the collection and returns what needs to be dropped/created.
//
// When forceRecreate is true (eg. because columns are being added/removed/
// redefined), ALL actual indexes are dropped and the declared ones are
// recreated - same as the regular collection update flow - to prevent
// column-rename side effects on the kept indexes.
func planCollectionIndexes(app App, collection *Collection, forceRecreate bool) (indexPlan, error) {
	plan := indexPlan{}

	actual, err := app.TableIndexes(collection.Name)
	if err != nil {
		return plan, err
	}

	errs := validation.Errors{}
	allDeclared := make([]dbutils.Index, 0, len(collection.Indexes))

	for i, raw := range collection.Indexes {
		parsed := dbutils.ParseIndex(raw)
		parsed.TableName = collection.Name
		parsed.SchemaName = ""

		if !parsed.IsValid() {
			errs[strconv.Itoa(i)] = validation.NewError(
				"validation_invalid_index_expression",
				"Invalid CREATE INDEX expression.",
			)
			continue
		}

		allDeclared = append(allDeclared, parsed)
	}

	if forceRecreate {
		plan.drop = make([]string, 0, len(actual))
		for name := range actual {
			plan.drop = append(plan.drop, name)
		}
		slices.Sort(plan.drop)
		plan.create = allDeclared

		if len(errs) > 0 {
			return plan, validation.Errors{"indexes": errs}
		}
		return plan, nil
	}

	usedActual := map[string]bool{}

	for _, parsed := range allDeclared {
		var actualKey string
		var actualSQL string
		for name, sql := range actual {
			if strings.EqualFold(name, parsed.IndexName) {
				actualKey = name
				actualSQL = sql
				break
			}
		}

		if actualSQL == "" {
			// missing declared index
			plan.create = append(plan.create, parsed)
			continue
		}

		usedActual[actualKey] = true

		actualParsed := dbutils.ParseIndex(actualSQL)
		if indexSignatureEquals(parsed, actualParsed) {
			continue // already up to date
		}

		// same name but different definition - recreate
		plan.drop = append(plan.drop, parsed.IndexName)
		plan.create = append(plan.create, parsed)
	}

	// indexes that exist physically but are no longer declared
	var extraNames []string
	for name := range actual {
		if !usedActual[name] {
			extraNames = append(extraNames, name)
		}
	}
	slices.Sort(extraNames)
	plan.drop = append(plan.drop, extraNames...)
	slices.Sort(plan.drop)
	plan.drop = slices.Compact(plan.drop)

	if len(errs) > 0 {
		return plan, validation.Errors{"indexes": errs}
	}

	return plan, nil
}

func indexSignatureEquals(a, b dbutils.Index) bool {
	if a.Unique != b.Unique {
		return false
	}

	if len(a.Columns) != len(b.Columns) {
		return false
	}

	for i := range a.Columns {
		ac, bc := a.Columns[i], b.Columns[i]
		if !strings.EqualFold(normalizeExpr(ac.Name), normalizeExpr(bc.Name)) {
			return false
		}
		if !strings.EqualFold(ac.Collate, bc.Collate) {
			return false
		}
		if !strings.EqualFold(ac.Sort, bc.Sort) {
			return false
		}
	}

	return strings.EqualFold(normalizeExpr(a.Where), normalizeExpr(b.Where))
}

func execIndexDrops(app App, plan indexPlan, report *ReconcileCollectionReport) error {
	for _, name := range plan.drop {
		query := fmt.Sprintf("DROP INDEX IF EXISTS [[%s]]", name)
		if _, err := app.DB().NewQuery(query).Execute(); err != nil {
			return fmt.Errorf("failed to drop index %s: %w", name, err)
		}
		report.DroppedIndexes = append(report.DroppedIndexes, name)
	}

	return nil
}

func execIndexCreates(app App, plan indexPlan, report *ReconcileCollectionReport) error {
	errs := validation.Errors{}

	for i, idx := range plan.create {
		if _, err := app.DB().NewQuery(idx.Build()).Execute(); err != nil {
			errs[strconv.Itoa(i)] = validation.NewError(
				"validation_invalid_index_expression",
				fmt.Sprintf("Failed to create index %s - %v.", idx.IndexName, err),
			)
			continue
		}
		report.CreatedIndexes = append(report.CreatedIndexes, idx.IndexName)
	}

	if len(errs) > 0 {
		return validation.Errors{"indexes": errs}
	}

	return nil
}

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------

func findTableInfoRow(rows map[string]*TableInfoRow, name string) *TableInfoRow {
	for n, row := range rows {
		if strings.EqualFold(n, name) {
			return row
		}
	}
	return nil
}

func findTableInfoRowByName(rows []*TableInfoRow, name string) *TableInfoRow {
	for _, row := range rows {
		if strings.EqualFold(row.Name, name) {
			return row
		}
	}
	return nil
}
