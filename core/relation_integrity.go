package core

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/tools/dbutils"
	"github.com/pocketbase/pocketbase/tools/inflector"
	"github.com/pocketbase/pocketbase/tools/list"
)

// relationIntegrityTableName is the dedicated ledger table where the durable,
// re-verifiable per-reference reconciliation conclusions are persisted.
const relationIntegrityTableName = "_relationIntegrityIssues"

const (
	// relationIntegrityActionUnset means the dangling reference(s) will be
	// stripped from the relation field value.
	relationIntegrityActionUnset = "unset"

	// relationIntegrityActionCascadeDelete means the referencing record will
	// be deleted because the field becomes empty and CascadeDelete is enabled.
	relationIntegrityActionCascadeDelete = "cascade_delete"

	// relationIntegrityActionBlocked means the dangling reference cannot be
	// auto-resolved (eg. a required non-cascade field becomes empty).
	relationIntegrityActionBlocked = "blocked"

	relationIntegrityStatusStaged   = "staged"
	relationIntegrityStatusResolved = "resolved"

	relationIntegrityReasonMissing  = "missing_relation_record"
	relationIntegrityReasonRequired = "required_reference"
)

var relationIntegrityColumns = []string{
	"id",
	"collectionId",
	"collectionName",
	"recordId",
	"fieldId",
	"fieldName",
	"targetCollectionId",
	"targetCollectionName",
	"refId",
	"action",
	"reason",
	"status",
	"created",
	"updated",
}

// RelationIntegrityIssue represents a single persisted reconciliation
// conclusion for a relation field reference (one row per record/field/refId).
type RelationIntegrityIssue struct {
	Id                   string `db:"id" json:"id"`
	CollectionId         string `db:"collectionId" json:"collectionId"`
	CollectionName       string `db:"collectionName" json:"collectionName"`
	RecordId             string `db:"recordId" json:"recordId"`
	FieldId              string `db:"fieldId" json:"fieldId"`
	FieldName            string `db:"fieldName" json:"fieldName"`
	TargetCollectionId   string `db:"targetCollectionId" json:"targetCollectionId"`
	TargetCollectionName string `db:"targetCollectionName" json:"targetCollectionName"`
	RefId                string `db:"refId" json:"refId"`
	Action               string `db:"action" json:"action"`
	Reason               string `db:"reason" json:"reason"`
	Status               string `db:"status" json:"status"`
	Created              string `db:"created" json:"created"`
	Updated              string `db:"updated" json:"updated"`
}

// RelationIntegrityReport contains the result of a single reconciliation run.
type RelationIntegrityReport struct {
	TargetCollectionId   string                    `json:"targetCollectionId"`
	TargetCollectionName string                    `json:"targetCollectionName"`
	CheckedReferences    int                       `json:"checkedReferences"`
	InvalidReferences    int                       `json:"invalidReferences"`
	Issues               []*RelationIntegrityIssue `json:"issues"`
	UpdatedRecordIds     []string                  `json:"updatedRecordIds"`
	DeletedRecordIds     []string                  `json:"deletedRecordIds"`
}

// RelationIntegrityBlockedError is returned when the reconciliation cannot
// be applied because one or more required relation fields would be left
// empty after removing the dangling references (and CascadeDelete is off).
//
// When such error is returned the whole batch is aborted without applying
// any changes, but the staged conclusions are still persisted in the ledger
// (see [App.FindRelationIntegrityIssues]) so that the blocking references
// can be inspected.
type RelationIntegrityBlockedError struct {
	TargetCollectionId   string
	TargetCollectionName string
	Issues               []*RelationIntegrityIssue
}

func (e *RelationIntegrityBlockedError) Error() string {
	details := make([]string, 0, 10)
	for i, issue := range e.Issues {
		if i >= 10 {
			details = append(details, fmt.Sprintf("... and %d more", len(e.Issues)-i))
			break
		}
		details = append(details, fmt.Sprintf(
			"%s.%s of record %s -> missing %s record %s",
			issue.CollectionName,
			issue.FieldName,
			issue.RecordId,
			issue.TargetCollectionName,
			issue.RefId,
		))
	}

	return fmt.Sprintf(
		"relation integrity reconcile for %q: %d required reference(s) cannot be resolved: %s",
		e.TargetCollectionName,
		len(e.Issues),
		strings.Join(details, "; "),
	)
}

type relationIntegrityRefEntry struct {
	collection *Collection
	fields     []*RelationField
}

type relationIntegrityPair struct {
	Id  string `db:"id"`
	Ref string `db:"ref"`
}

// ReconcileRelationIntegrity verifies all relation field references pointing
// to the provided target collection and repairs the ones that reference
// missing records.
//
// The reconciliation is scoped to the target collection:
//   - references whose related records still exist are not touched;
//   - relation fields pointing to other collections are not even scanned.
//
// For each invalid reference a durable conclusion is persisted in the
// _relationIntegrityIssues ledger before any data change is applied:
//   - "unset": the dangling id(s) are stripped from the field value;
//   - "cascade_delete": the referencing record is deleted (the regular record
//     delete flow is used, so its own outgoing cascades run in the same batch)
//     when a CascadeDelete field becomes empty;
//   - "blocked": a required non-cascade field would become empty.
//
// If at least one reference cannot be resolved ("blocked"), the whole batch
// fails and no record is modified.
//
// All data modifications are applied in a single transaction (all or nothing).
// The run is idempotent and convergent: rerunning after a crash or after a
// successful run produces the same final state and leaves no pending work.
//
// After a successful run the relation field storage is normalized, meaning
// that both record reads and reference-based filters see consistent results.
func (app *BaseApp) ReconcileRelationIntegrity(target *Collection) (*RelationIntegrityReport, error) {
	if target == nil {
		return nil, errors.New("relation integrity reconcile: target collection is not set")
	}

	if target.IsView() {
		return nil, errors.New("relation integrity reconcile: view collections cannot be reconciled because they don't store their own records")
	}

	refs, err := app.FindCachedCollectionReferences(target)
	if err != nil {
		return nil, fmt.Errorf("relation integrity reconcile: failed to fetch relation references: %w", err)
	}

	entries := make([]relationIntegrityRefEntry, 0, len(refs))
	for c, fields := range refs {
		// view collections don't store records (their data is derived from
		// their view query), so there is nothing to repair on their side.
		if c.IsView() {
			continue
		}

		relFields := make([]*RelationField, 0, len(fields))
		for _, f := range fields {
			if rf, ok := f.(*RelationField); ok {
				relFields = append(relFields, rf)
			}
		}
		if len(relFields) == 0 {
			continue
		}

		sort.Slice(relFields, func(i, j int) bool {
			return relFields[i].Name < relFields[j].Name
		})

		entries = append(entries, relationIntegrityRefEntry{collection: c, fields: relFields})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].collection.Name < entries[j].collection.Name
	})

	report := &RelationIntegrityReport{
		TargetCollectionId:   target.Id,
		TargetCollectionName: target.Name,
		Issues:               []*RelationIntegrityIssue{},
		UpdatedRecordIds:     []string{},
		DeletedRecordIds:     []string{},
	}

	// Phase 1: analysis.
	//
	// Build and persist the staged conclusions in a separate committed
	// transaction. Nothing from the collections data is modified here.
	// This durable checkpoint is what makes the operation crash-safe:
	// if the process dies before/during the apply phase, the next run
	// simply recomputes the same deterministic conclusions.
	var staged []*RelationIntegrityIssue
	txErr := app.RunInTransaction(func(txApp App) error {
		if err := ensureRelationIntegrityTable(txApp); err != nil {
			return err
		}

		// reset any previously staged (but not applied) conclusion for the scope;
		// resolved rows are kept as audit trail.
		_, err := txApp.NonconcurrentDB().
			Delete(relationIntegrityTableName, dbx.HashExp{
				"targetCollectionId": target.Id,
				"status":             relationIntegrityStatusStaged,
			}).
			Execute()
		if err != nil {
			return err
		}

		rows, checked, err := analyzeRelationIntegrity(txApp, target, entries)
		if err != nil {
			return err
		}

		// resolve per-record verdicts before persisting them so that
		// a cascade delete from one field overrides a "blocked" verdict
		// of another field of the same record.
		normalizeRelationIntegrityRecordActions(rows)
		sortRelationIntegrityIssues(rows)

		now := relationIntegrityNow()
		for _, row := range rows {
			row.Updated = now
			if row.Created == "" {
				row.Created = now
			}
			if err := upsertRelationIntegrityIssue(txApp, row); err != nil {
				return err
			}
		}

		staged = rows
		report.CheckedReferences = checked

		return nil
	})
	if txErr != nil {
		return nil, txErr
	}

	report.Issues = staged
	report.InvalidReferences = len(staged)

	var blocked []*RelationIntegrityIssue
	for _, row := range staged {
		if row.Action == relationIntegrityActionBlocked {
			blocked = append(blocked, row)
		}
	}
	if len(blocked) > 0 {
		// nothing is applied - the whole batch fails as requested.
		return report, &RelationIntegrityBlockedError{
			TargetCollectionId:   target.Id,
			TargetCollectionName: target.Name,
			Issues:               blocked,
		}
	}

	if len(staged) == 0 {
		return report, nil
	}

	// Phase 2: apply.
	//
	// All data changes + the ledger resolution happen in a single transaction,
	// ensuring that either the entire batch takes effect or nothing does.
	// Each conclusion is re-verified against the fresh in-transaction state
	// so that concurrent changes (or reruns after a crash) always converge.
	txErr = app.RunInTransaction(func(txApp App) error {
		issuesByCollection := map[string]map[string][]*RelationIntegrityIssue{}
		for _, row := range staged {
			byRecord, ok := issuesByCollection[row.CollectionId]
			if !ok {
				byRecord = map[string][]*RelationIntegrityIssue{}
				issuesByCollection[row.CollectionId] = byRecord
			}
			byRecord[row.RecordId] = append(byRecord[row.RecordId], row)
		}

		collectionIds := make([]string, 0, len(issuesByCollection))
		for id := range issuesByCollection {
			collectionIds = append(collectionIds, id)
		}
		sort.Strings(collectionIds)

		for _, collectionId := range collectionIds {
			refCollection, err := txApp.FindCollectionByNameOrId(collectionId)
			if err != nil {
				return err
			}

			byRecord := issuesByCollection[collectionId]
			recordIds := make([]string, 0, len(byRecord))
			for id := range byRecord {
				recordIds = append(recordIds, id)
			}
			sort.Strings(recordIds)

			for _, recordId := range recordIds {
				record, err := txApp.FindRecordById(refCollection, recordId)
				if err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						// already deleted (eg. by an earlier cascade in the same batch)
						continue
					}
					return err
				}

				fields := make([]*RelationField, 0, len(byRecord[recordId]))
				seenFields := map[string]struct{}{}
				for _, row := range byRecord[recordId] {
					if _, ok := seenFields[row.FieldId]; ok {
						continue
					}
					seenFields[row.FieldId] = struct{}{}
					if rf, ok := refCollection.Fields.GetById(row.FieldId).(*RelationField); ok {
						fields = append(fields, rf)
					}
				}
				sort.Slice(fields, func(i, j int) bool {
					return fields[i].Name < fields[j].Name
				})

				allValues := []string{}
				for _, f := range fields {
					allValues = append(allValues, record.GetStringSlice(f.Name)...)
				}

				existing, err := existingRelationIds(txApp, target, allValues)
				if err != nil {
					return err
				}

				willDelete := false
				isBlocked := false
				hasChanges := false

				for _, f := range fields {
					values := record.GetStringSlice(f.Name)
					remaining := make([]string, 0, len(values))
					missingCount := 0
					for _, v := range values {
						if _, ok := existing[v]; ok {
							remaining = append(remaining, v)
						} else {
							missingCount++
						}
					}

					if missingCount == 0 {
						continue // already fixed (eg. concurrent change)
					}

					action, _ := relationFieldVerdict(f, len(remaining))
					switch action {
					case relationIntegrityActionCascadeDelete:
						willDelete = true
					case relationIntegrityActionBlocked:
						isBlocked = true
					default:
						record.Set(f.Name, remaining)
						hasChanges = true
					}
				}

				// a cascade delete from one field resolves the blocked state
				// of the other fields of the same record.
				if isBlocked && !willDelete {
					return &RelationIntegrityBlockedError{
						TargetCollectionId:   target.Id,
						TargetCollectionName: target.Name,
						Issues:               byRecord[recordId],
					}
				}

				if willDelete {
					if err := txApp.Delete(record); err != nil {
						return err
					}
					report.DeletedRecordIds = append(report.DeletedRecordIds, recordId)
					continue
				}

				if hasChanges {
					// without validation for consistency with the regular cascade
					// delete flow (other relation fields may still reference deleted records)
					if err := txApp.SaveNoValidate(record); err != nil {
						return err
					}
					report.UpdatedRecordIds = append(report.UpdatedRecordIds, recordId)
				}
			}
		}

		_, err = txApp.NonconcurrentDB().
			Update(
				relationIntegrityTableName,
				dbx.Params{
					"status":  relationIntegrityStatusResolved,
					"updated": relationIntegrityNow(),
				},
				dbx.HashExp{
					"targetCollectionId": target.Id,
					"status":             relationIntegrityStatusStaged,
				},
			).
			Execute()
		if err != nil {
			return err
		}

		return nil
	})
	if txErr != nil {
		return report, txErr
	}

	sort.Strings(report.UpdatedRecordIds)
	sort.Strings(report.DeletedRecordIds)

	return report, nil
}

// FindRelationIntegrityIssues returns the persisted reconciliation conclusions
// for the specified target collection.
//
// Optionally filter by status ("staged" and/or "resolved").
//
// It returns empty slice if the ledger table doesn't exist yet.
func (app *BaseApp) FindRelationIntegrityIssues(targetCollectionId string, statuses ...string) ([]*RelationIntegrityIssue, error) {
	if !app.HasTable(relationIntegrityTableName) {
		return []*RelationIntegrityIssue{}, nil
	}

	columns := make([]string, len(relationIntegrityColumns))
	for i, c := range relationIntegrityColumns {
		columns[i] = "[[" + c + "]]"
	}

	query := app.DB().
		Select(strings.Join(columns, ",")).
		From(relationIntegrityTableName).
		AndWhere(dbx.HashExp{"targetCollectionId": targetCollectionId})

	if uniqueStatuses := list.NonzeroUniques(statuses); len(uniqueStatuses) > 0 {
		query.AndWhere(dbx.In("status", list.ToInterfaceSlice(uniqueStatuses)...))
	}

	rows := []*RelationIntegrityIssue{}
	if err := query.
		OrderBy("[[collectionName]]", "[[recordId]]", "[[fieldName]]", "[[refId]]").
		All(&rows); err != nil {
		return nil, err
	}

	return rows, nil
}

// analyzeRelationIntegrity scans the provided referencing collections/fields
// and builds a deterministic conclusion for each invalid reference.
//
// NB! It is expected to be called from inside of a transaction.
func analyzeRelationIntegrity(app App, target *Collection, entries []relationIntegrityRefEntry) ([]*RelationIntegrityIssue, int, error) {
	issues := []*RelationIntegrityIssue{}
	checked := 0

	for _, entry := range entries {
		refCollection := entry.collection
		refTableName := inflector.Columnify(refCollection.Name)

		// fieldId -> recordId -> set of SQL-level dangling ref ids
		candidates := map[string]map[string]map[string]struct{}{}

		for _, f := range entry.fields {
			col := inflector.Columnify(f.Name)

			total, err := countRelationRefs(app, refTableName, col, f.IsMultiple())
			if err != nil {
				return nil, 0, fmt.Errorf("[%s.%s] failed to count references: %w", refCollection.Name, f.Name, err)
			}
			checked += total

			pairs, err := scanDanglingPairs(app, target, refTableName, col, f.IsMultiple())
			if err != nil {
				return nil, 0, fmt.Errorf("[%s.%s] failed to scan references: %w", refCollection.Name, f.Name, err)
			}

			byRecord, ok := candidates[f.Id]
			if !ok {
				byRecord = map[string]map[string]struct{}{}
				candidates[f.Id] = byRecord
			}

			for _, pair := range pairs {
				set, ok := byRecord[pair.Id]
				if !ok {
					set = map[string]struct{}{}
					byRecord[pair.Id] = set
				}
				set[pair.Ref] = struct{}{}
			}
		}

		candidateRecordIds := map[string]struct{}{}
		for _, byRecord := range candidates {
			for id := range byRecord {
				candidateRecordIds[id] = struct{}{}
			}
		}

		// reload the candidate records through the ORM so that the final
		// conclusion is always based on the same value normalization that
		// the application uses when reading/saving records.
		for _, chunk := range chunkStrings(sortedStringSet(candidateRecordIds), 500) {
			records, err := app.FindRecordsByIds(refCollection, chunk)
			if err != nil {
				return nil, 0, err
			}

			recordsById := map[string]*Record{}
			for _, r := range records {
				recordsById[r.Id] = r
			}

			allValues := []string{}
			valuesByRecord := map[string]map[string][]string{}
			for _, id := range chunk {
				record, ok := recordsById[id]
				if !ok {
					continue // vanished between the scan and the reload
				}

				valuesByField := map[string][]string{}
				for _, f := range entry.fields {
					vals := record.GetStringSlice(f.Name)
					valuesByField[f.Id] = vals
					allValues = append(allValues, vals...)
				}
				valuesByRecord[id] = valuesByField
			}

			existing, err := existingRelationIds(app, target, allValues)
			if err != nil {
				return nil, 0, err
			}

			for _, id := range chunk {
				valuesByField, ok := valuesByRecord[id]
				if !ok {
					continue
				}

				for _, f := range entry.fields {
					values := valuesByField[f.Id]

					missing := []string{}
					remaining := 0
					for _, v := range values {
						if _, ok := existing[v]; ok {
							remaining++
						} else {
							missing = append(missing, v)
						}
					}

					if len(missing) == 0 {
						continue
					}

					action, reason := relationFieldVerdict(f, remaining)
					for _, refId := range missing {
						issues = append(issues, newRelationIntegrityIssue(
							refCollection, id, f, target, refId, action, reason,
						))
					}
				}
			}
		}
	}

	sortRelationIntegrityIssues(issues)

	return issues, checked, nil
}

// relationFieldVerdict returns the conclusion action for a relation field
// whose dangling references leave `remaining` still-existing references.
//
// The rules mirror the regular record delete cascade behavior
// (see deleteRefRecords).
func relationFieldVerdict(f *RelationField, remaining int) (action string, reason string) {
	if remaining == 0 {
		if f.CascadeDelete {
			return relationIntegrityActionCascadeDelete, relationIntegrityReasonMissing
		}
		if f.Required {
			return relationIntegrityActionBlocked, relationIntegrityReasonRequired
		}
	}

	return relationIntegrityActionUnset, relationIntegrityReasonMissing
}

func countRelationRefs(app App, table, col string, multiple bool) (int, error) {
	var total int

	if multiple {
		raw := fmt.Sprintf(
			"SELECT count(*) FROM {{%s}}, %s",
			table,
			dbutils.JSONEach(table+"."+col),
		)
		if err := app.DB().NewQuery(raw).Row(&total); err != nil {
			return 0, err
		}
		return total, nil
	}

	err := app.DB().
		Select("count(*)").
		From(table).
		AndWhere(dbx.NewExp(fmt.Sprintf("[[%s.%s]] != ''", table, col))).
		Row(&total)

	return total, err
}

// scanDanglingPairs returns (recordId, refId) pairs for stored relation
// values for which there is no matching record in the target collection.
func scanDanglingPairs(app App, target *Collection, table, col string, multiple bool) ([]relationIntegrityPair, error) {
	targetTable := inflector.Columnify(target.Name)

	var raw string
	if multiple {
		raw = fmt.Sprintf(`
SELECT [[%[1]s.id]] AS [[id]], [[__je__.value]] AS [[ref]]
FROM {{%[1]s}}, %[2]s {{__je__}}
WHERE NOT EXISTS (SELECT 1 FROM {{%[3]s}} {{__p__}} WHERE [[__p__.id]] = [[__je__.value]])
ORDER BY [[%[1]s.id]], [[__je__.value]]`,
			table,
			dbutils.JSONEach(table+"."+col),
			targetTable,
		)
	} else {
		raw = fmt.Sprintf(`
SELECT [[%[1]s.id]] AS [[id]], [[%[1]s.%[3]s]] AS [[ref]]
FROM {{%[1]s}}
WHERE [[%[1]s.%[3]s]] != ''
AND NOT EXISTS (SELECT 1 FROM {{%[2]s}} {{__p__}} WHERE [[__p__.id]] = [[%[1]s.%[3]s]])
ORDER BY [[%[1]s.id]]`,
			table,
			targetTable,
			col,
		)
	}

	const batchSize = 8000

	out := []relationIntegrityPair{}
	offset := 0
	for {
		rows := []relationIntegrityPair{}
		err := app.DB().
			NewQuery(raw + " LIMIT {:limit} OFFSET {:offset}").
			Bind(dbx.Params{"limit": batchSize, "offset": offset}).
			All(&rows)
		if err != nil {
			return nil, err
		}

		out = append(out, rows...)

		if len(rows) < batchSize {
			break
		}
		offset += batchSize
	}

	return out, nil
}

// existingRelationIds returns the set of ids from the provided list that
// actually exist in the target collection.
func existingRelationIds(app App, target *Collection, ids []string) (map[string]struct{}, error) {
	existing := map[string]struct{}{}

	unique := list.NonzeroUniques(ids)
	if len(unique) == 0 {
		return existing, nil
	}

	table := inflector.Columnify(target.Name)

	for _, chunk := range chunkStrings(unique, 800) {
		found := []string{}
		err := app.DB().
			Select("id").
			From(table).
			AndWhere(dbx.In("id", list.ToInterfaceSlice(chunk)...)).
			Column(&found)
		if err != nil {
			return nil, err
		}

		for _, id := range found {
			existing[id] = struct{}{}
		}
	}

	return existing, nil
}

func newRelationIntegrityIssue(
	refCollection *Collection,
	recordId string,
	field *RelationField,
	target *Collection,
	refId string,
	action string,
	reason string,
) *RelationIntegrityIssue {
	key := strings.Join([]string{
		target.Id,
		refCollection.Id,
		recordId,
		field.Id,
		refId,
	}, "\x00")
	sum := sha256.Sum256([]byte(key))

	return &RelationIntegrityIssue{
		Id:                   hex.EncodeToString(sum[:]),
		CollectionId:         refCollection.Id,
		CollectionName:       refCollection.Name,
		RecordId:             recordId,
		FieldId:              field.Id,
		FieldName:            field.Name,
		TargetCollectionId:   target.Id,
		TargetCollectionName: target.Name,
		RefId:                refId,
		Action:               action,
		Reason:               reason,
		Status:               relationIntegrityStatusStaged,
	}
}

func ensureRelationIntegrityTable(app App) error {
	rawTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS {{%s}} (
  [[id]] TEXT PRIMARY KEY NOT NULL,
  [[collectionId]] TEXT NOT NULL,
  [[collectionName]] TEXT NOT NULL,
  [[recordId]] TEXT NOT NULL,
  [[fieldId]] TEXT NOT NULL,
  [[fieldName]] TEXT NOT NULL,
  [[targetCollectionId]] TEXT NOT NULL,
  [[targetCollectionName]] TEXT NOT NULL,
  [[refId]] TEXT NOT NULL,
  [[action]] TEXT NOT NULL,
  [[reason]] TEXT NOT NULL,
  [[status]] TEXT NOT NULL,
  [[created]] TEXT NOT NULL,
  [[updated]] TEXT NOT NULL
)`, relationIntegrityTableName)

	if _, err := app.NonconcurrentDB().NewQuery(rawTable).Execute(); err != nil {
		return err
	}

	rawIndex := fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS {{%s_target_status_idx}} ON {{%s}} ([[targetCollectionId]], [[status]])`,
		relationIntegrityTableName,
		relationIntegrityTableName,
	)
	if _, err := app.NonconcurrentDB().NewQuery(rawIndex).Execute(); err != nil {
		return err
	}

	return nil
}

func upsertRelationIntegrityIssue(app App, row *RelationIntegrityIssue) error {
	raw := fmt.Sprintf(`
INSERT INTO {{%s}} (
  [[id]], [[collectionId]], [[collectionName]], [[recordId]], [[fieldId]], [[fieldName]],
  [[targetCollectionId]], [[targetCollectionName]], [[refId]], [[action]], [[reason]],
  [[status]], [[created]], [[updated]]
) VALUES (
  {:id}, {:collectionId}, {:collectionName}, {:recordId}, {:fieldId}, {:fieldName},
  {:targetCollectionId}, {:targetCollectionName}, {:refId}, {:action}, {:reason},
  {:status}, {:created}, {:updated}
)
ON CONFLICT([[id]]) DO UPDATE SET
  [[collectionName]] = excluded.[[collectionName]],
  [[recordId]] = excluded.[[recordId]],
  [[fieldName]] = excluded.[[fieldName]],
  [[targetCollectionName]] = excluded.[[targetCollectionName]],
  [[action]] = excluded.[[action]],
  [[reason]] = excluded.[[reason]],
  [[status]] = {:status},
  [[updated]] = {:updated}`, relationIntegrityTableName)

	_, err := app.NonconcurrentDB().
		NewQuery(raw).
		Bind(dbx.Params{
			"id":                   row.Id,
			"collectionId":         row.CollectionId,
			"collectionName":       row.CollectionName,
			"recordId":             row.RecordId,
			"fieldId":              row.FieldId,
			"fieldName":            row.FieldName,
			"targetCollectionId":   row.TargetCollectionId,
			"targetCollectionName": row.TargetCollectionName,
			"refId":                row.RefId,
			"action":               row.Action,
			"reason":               row.Reason,
			"status":               row.Status,
			"created":              row.Created,
			"updated":              row.Updated,
		}).
		Execute()

	return err
}

func groupIssuesByRecord(issues []*RelationIntegrityIssue) map[string][]*RelationIntegrityIssue {
	groups := map[string][]*RelationIntegrityIssue{}
	for _, issue := range issues {
		key := issue.CollectionId + "\x00" + issue.RecordId
		groups[key] = append(groups[key], issue)
	}
	return groups
}

// normalizeRelationIntegrityRecordActions resolves the final per-record
// verdicts: a cascade delete triggered by one field also resolves a
// "blocked" verdict of another field of the same record because the
// record will not exist anymore.
func normalizeRelationIntegrityRecordActions(issues []*RelationIntegrityIssue) {
	for _, rows := range groupIssuesByRecord(issues) {
		hasCascade := false
		for _, row := range rows {
			if row.Action == relationIntegrityActionCascadeDelete {
				hasCascade = true
				break
			}
		}
		if !hasCascade {
			continue
		}
		for _, row := range rows {
			row.Action = relationIntegrityActionCascadeDelete
			row.Reason = relationIntegrityReasonMissing
		}
	}
}

func sortRelationIntegrityIssues(issues []*RelationIntegrityIssue) {
	sort.Slice(issues, func(i, j int) bool {
		a, b := issues[i], issues[j]
		if a.CollectionName != b.CollectionName {
			return a.CollectionName < b.CollectionName
		}
		if a.RecordId != b.RecordId {
			return a.RecordId < b.RecordId
		}
		if a.FieldName != b.FieldName {
			return a.FieldName < b.FieldName
		}
		return a.RefId < b.RefId
	})
}

func sortedStringSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func chunkStrings(items []string, size int) [][]string {
	if size <= 0 {
		return [][]string{items}
	}

	chunks := [][]string{}
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[i:end])
	}

	return chunks
}

func relationIntegrityNow() string {
	return time.Now().UTC().Format(time.RFC3339)
}
