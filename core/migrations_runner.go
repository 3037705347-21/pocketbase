package core

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/tools/osutils"
	"github.com/spf13/cast"
)

var AppMigrations MigrationsList
var SystemMigrations MigrationsList

const DefaultMigrationsTable = "_migrations"

// reconcileTempPrefix is used as a temporary file name prefix when renaming
// history entries to support cyclic renames (eg. a <-> b) without violating
// the primary key constraint.
const reconcileTempPrefix = "__pb_reconcile_tmp_"

// Reconciliation statuses returned by [MigrationsRunner.ReconcileAppliedMigrations].
const (
	// MigrationHistoryConsistent means that the applied migration history
	// entry corresponds to an unchanged current migration source.
	MigrationHistoryConsistent = "consistent"

	// MigrationHistoryRenamed means that the applied migration content is
	// still present but the source file was renamed.
	MigrationHistoryRenamed = "renamed"

	// MigrationHistoryContentChanged means that the migration file still
	// exists but its content differs from what was applied.
	MigrationHistoryContentChanged = "contentChanged"

	// MigrationHistoryMissing means that the applied migration source
	// (same name or content identity) no longer exists.
	MigrationHistoryMissing = "missing"
)

// ErrUndeterminableMigrationsHistory is returned by the history reconciliation
// when applied entries cannot be unambiguously mapped to the current
// migration sources (eg. legacy entries without content identity that could
// be either renames with content edits or genuinely new/missing migrations).
//
// In that case no changes are written to the migrations history and the
// conflict has to be resolved manually.
var ErrUndeterminableMigrationsHistory = errors.New(
	"unable to reconcile the migrations history with the current migration sources",
)

// ReconciledMigration represents a single reconciled applied migration entry.
type ReconciledMigration struct {
	// File is the applied migration file name stored in the history table.
	File string

	// NewFile is the current migration source file name when the entry was renamed.
	NewFile string

	// Identity is the content identity after the reconciliation.
	Identity string

	// Status is one of the MigrationHistory* constants.
	Status string
}

// ReconcileMigrationsResult is the result of
// [MigrationsRunner.ReconcileAppliedMigrations].
type ReconcileMigrationsResult struct {
	Items []ReconciledMigration
}

// MigrationsRunner defines a simple struct for managing the execution of db migrations.
type MigrationsRunner struct {
	app            App
	tableName      string
	migrationsList MigrationsList
	inited         bool

	// reconcile enables identity-based history reconciliation before Up execution.
	reconcile bool
}

// NewMigrationsRunner creates and initializes a new db migrations MigrationsRunner instance.
func NewMigrationsRunner(app App, migrationsList MigrationsList) *MigrationsRunner {
	return &MigrationsRunner{
		app:            app,
		migrationsList: migrationsList,
		tableName:      DefaultMigrationsTable,
	}
}

// WithHistoryReconciliation enables the identity-based reconciliation of the
// migrations history table before executing [MigrationsRunner.Up] so that
// renamed, content-changed or missing migration sources are converged first
// and the subsequent migration execution directly adopts the reconciled state.
func (r *MigrationsRunner) WithHistoryReconciliation() *MigrationsRunner {
	r.reconcile = true
	return r
}

// Run interactively executes the current runner with the provided args.
//
// The following commands are supported:
//   - up           - applies all migrations
//   - down [n]     - reverts the last n (default 1) applied migrations
//   - history-sync - reconciles the migrations table with the runner's migrations list
//     based on the migrations content identity (handles renamed, content-changed
//     and missing migration files; fails if the mapping cannot be determined)
func (r *MigrationsRunner) Run(args ...string) error {
	if err := r.initMigrationsTable(); err != nil {
		return err
	}

	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "up":
		applied, err := r.Up()
		if err != nil {
			return err
		}

		if len(applied) == 0 {
			color.Green("No new migrations to apply.")
		} else {
			for _, file := range applied {
				color.Green("Applied %s", file)
			}
		}

		return nil
	case "down":
		toRevertCount := 1
		if len(args) > 1 {
			toRevertCount = cast.ToInt(args[1])
			if toRevertCount < 0 {
				// revert all applied migrations
				toRevertCount = len(r.migrationsList.Items())
			}
		}

		names, err := r.lastAppliedMigrations(toRevertCount)
		if err != nil {
			return err
		}

		confirm := osutils.YesNoPrompt(fmt.Sprintf(
			"\n%v\nDo you really want to revert the last %d applied migration(s)?",
			strings.Join(names, "\n"),
			toRevertCount,
		), false)
		if !confirm {
			fmt.Println("The command has been cancelled")
			return nil
		}

		reverted, err := r.Down(toRevertCount)
		if err != nil {
			return err
		}

		if len(reverted) == 0 {
			color.Green("No migrations to revert.")
		} else {
			for _, file := range reverted {
				color.Green("Reverted %s", file)
			}
		}

		return nil
	case "history-sync":
		result, err := r.ReconcileAppliedMigrations()
		if err != nil {
			return err
		}

		for _, item := range result.Items {
			switch item.Status {
			case MigrationHistoryRenamed:
				color.Yellow("Renamed applied migration %q to %q.", item.File, item.NewFile)
			case MigrationHistoryContentChanged:
				color.Yellow(
					"Applied migration %q content differs from the applied one - refreshed its identity (it will not be re-executed).",
					item.File,
				)
			case MigrationHistoryMissing:
				color.Yellow("Removed stale applied migration entry %q.", item.File)
			}
		}

		color.Green("The %s table was reconciled with the available migrations.", r.tableName)
		return nil
	default:
		return fmt.Errorf("unsupported command: %q", cmd)
	}
}

// Up executes all unapplied migrations for the provided runner.
//
// On success returns list with the applied migrations file names.
func (r *MigrationsRunner) Up() ([]string, error) {
	if err := r.initMigrationsTable(); err != nil {
		return nil, err
	}

	applied := []string{}

	err := r.app.AuxRunInTransaction(func(txApp App) error {
		return txApp.RunInTransaction(func(txApp App) error {
			// converge the history table first (renames, content changes, missing sources)
			// so that the migration execution below directly adopts the reconciled state
			if r.reconcile {
				if _, err := r.reconcileAppliedMigrations(txApp); err != nil {
					return err
				}
			}

			for _, m := range r.migrationsList.Items() {
				// applied migrations check
				if r.isMigrationApplied(txApp, m.File) {
					if m.ReapplyCondition == nil {
						continue // no need to reapply
					}

					shouldReapply, err := m.ReapplyCondition(txApp, r, m.File)
					if err != nil {
						return err
					}
					if !shouldReapply {
						continue
					}

					// clear previous history stored entry
					// (it will be recreated after successful execution)
					r.saveRevertedMigration(txApp, m.File)
				}

				// ignore empty Up action
				if m.Up != nil {
					if err := m.Up(txApp); err != nil {
						return fmt.Errorf("failed to apply migration %s: %w", m.File, err)
					}
				}

				if err := r.saveAppliedMigration(txApp, m.File, m.Identity); err != nil {
					return fmt.Errorf("failed to save applied migration info for %s: %w", m.File, err)
				}

				applied = append(applied, m.File)
			}

			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return applied, nil
}

// Down reverts the last `toRevertCount` applied migrations
// (in the order they were applied).
//
// On success returns list with the reverted migrations file names.
func (r *MigrationsRunner) Down(toRevertCount int) ([]string, error) {
	if err := r.initMigrationsTable(); err != nil {
		return nil, err
	}

	reverted := make([]string, 0, toRevertCount)

	names, appliedErr := r.lastAppliedMigrations(toRevertCount)
	if appliedErr != nil {
		return nil, appliedErr
	}

	err := r.app.AuxRunInTransaction(func(txApp App) error {
		return txApp.RunInTransaction(func(txApp App) error {
			for _, name := range names {
				for _, m := range r.migrationsList.Items() {
					if m.File != name {
						continue
					}

					// revert limit reached
					if toRevertCount-len(reverted) <= 0 {
						return nil
					}

					// ignore empty Down action
					if m.Down != nil {
						if err := m.Down(txApp); err != nil {
							return fmt.Errorf("failed to revert migration %s: %w", m.File, err)
						}
					}

					if err := r.saveRevertedMigration(txApp, m.File); err != nil {
						return fmt.Errorf("failed to save reverted migration info for %s: %w", m.File, err)
					}

					reverted = append(reverted, m.File)
				}
			}
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	return reverted, nil
}

// RemoveMissingAppliedMigrations removes the db entries of all applied migrations
// that are not listed in the runner's migrations list.
func (r *MigrationsRunner) RemoveMissingAppliedMigrations() error {
	loadedMigrations := r.migrationsList.Items()

	names := make([]any, len(loadedMigrations))
	for i, migration := range loadedMigrations {
		names[i] = migration.File
	}

	_, err := r.app.DB().Delete(r.tableName, dbx.Not(dbx.HashExp{
		"file": names,
	})).Execute()

	return err
}

func (r *MigrationsRunner) initMigrationsTable() error {
	if r.inited {
		return nil // already inited
	}

	rawQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS {{%s}} (file VARCHAR(255) PRIMARY KEY NOT NULL, applied INTEGER NOT NULL, identity VARCHAR(64) NOT NULL DEFAULT '')",
		r.tableName,
	)

	if _, err := r.app.DB().NewQuery(rawQuery).Execute(); err != nil {
		return err
	}

	// ensure the identity column exists for history tables created before it was introduced
	columns, err := r.app.TableColumns(r.tableName)
	if err != nil {
		return err
	}

	for _, column := range columns {
		if column == "identity" {
			r.inited = true
			return nil
		}
	}

	if _, err := r.app.DB().NewQuery(fmt.Sprintf(
		"ALTER TABLE {{%s}} ADD COLUMN identity VARCHAR(64) NOT NULL DEFAULT ''",
		r.tableName,
	)).Execute(); err != nil {
		return err
	}

	r.inited = true

	return nil
}

func (r *MigrationsRunner) isMigrationApplied(txApp App, file string) bool {
	var exists int

	err := txApp.DB().Select("(1)").
		From(r.tableName).
		Where(dbx.HashExp{"file": file}).
		Limit(1).
		Row(&exists)

	return err == nil && exists > 0
}

func (r *MigrationsRunner) saveAppliedMigration(txApp App, file string, identity string) error {
	_, err := txApp.DB().Insert(r.tableName, dbx.Params{
		"file":     file,
		"applied":  time.Now().UnixMicro(),
		"identity": identity,
	}).Execute()

	return err
}

func (r *MigrationsRunner) saveRevertedMigration(txApp App, file string) error {
	_, err := txApp.DB().Delete(r.tableName, dbx.HashExp{"file": file}).Execute()

	return err
}

func (r *MigrationsRunner) lastAppliedMigrations(limit int) ([]string, error) {
	var files = make([]string, 0, limit)

	loadedMigrations := r.migrationsList.Items()

	names := make([]any, len(loadedMigrations))
	for i, migration := range loadedMigrations {
		names[i] = migration.File
	}

	err := r.app.DB().Select("file").
		From(r.tableName).
		Where(dbx.Not(dbx.HashExp{"applied": nil})).
		AndWhere(dbx.HashExp{"file": names}).
		// unify microseconds and seconds applied time for backward compatibility
		OrderBy("substr(applied||'0000000000000000', 0, 17) DESC").
		AndOrderBy("file DESC").
		Limit(int64(limit)).
		Column(&files)

	if err != nil {
		return nil, err
	}

	return files, nil
}

type migrationHistoryRow struct {
	File     string `db:"file"`
	Applied  int64  `db:"applied"`
	Identity string `db:"identity"`
}

// reconciliation plan entry computed for a single applied history row
type migrationReconcileEntry struct {
	row     migrationHistoryRow
	status  string
	newName string // populated for renamed entries
	newID   string // refreshed/backfilled identity after the reconciliation
}

// ReconcileAppliedMigrations reconciles the migrations history table with the
// runner's current migration sources based on their content identity.
//
// Each applied history entry is classified into one of:
//   - consistent - the source file name and content identity match
//   - renamed - the content identity exists but under a different file name
//   - contentChanged - the same file exists but its content identity differs
//   - missing - neither the file name nor the identity can be found
//
// All convergence writes (history renames, identity refreshes and stale entry
// deletions) are executed in a single transaction.
//
// If an entry cannot be unambiguously mapped to a current source (eg. legacy
// entry without identity that could be either a renamed-and-edited migration
// or a missing one with new sources added), the whole batch fails with
// [ErrUndeterminableMigrationsHistory] and nothing is written to the table.
func (r *MigrationsRunner) ReconcileAppliedMigrations() (*ReconcileMigrationsResult, error) {
	if err := r.initMigrationsTable(); err != nil {
		return nil, err
	}

	var result *ReconcileMigrationsResult

	err := r.app.AuxRunInTransaction(func(txApp App) error {
		return txApp.RunInTransaction(func(txApp App) error {
			var execErr error
			result, execErr = r.reconcileAppliedMigrations(txApp)
			return execErr
		})
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *MigrationsRunner) reconcileAppliedMigrations(txApp App) (*ReconcileMigrationsResult, error) {
	sources := r.migrationsList.Items()

	byName := make(map[string]*Migration, len(sources))
	byIdentity := map[string][]*Migration{}
	for _, m := range sources {
		byName[m.File] = m
		if m.Identity != "" {
			byIdentity[m.Identity] = append(byIdentity[m.Identity], m)
		}
	}

	rows := []migrationHistoryRow{}
	if err := txApp.DB().
		Select("file", "applied", "identity").
		From(r.tableName).
		OrderBy("file ASC").
		All(&rows); err != nil {
		return nil, err
	}

	historyByName := make(map[string]migrationHistoryRow, len(rows))
	for _, row := range rows {
		historyByName[row.File] = row
	}

	entries := make([]migrationReconcileEntry, 0, len(rows))
	undeterminable := []string{}

	for _, h := range rows {
		if strings.HasPrefix(h.File, reconcileTempPrefix) {
			undeterminable = append(undeterminable, fmt.Sprintf(
				"- %q conflicts with the internal reconciliation temporary prefix %q; please remove/rename it manually",
				h.File, reconcileTempPrefix,
			))
			continue
		}

		sName := byName[h.File]

		// identity-based matching takes precedence so that renames
		// (including the case where the old name was reused by new content)
		// are resolved based on what was actually applied
		if h.Identity != "" {
			matches := byIdentity[h.Identity]

			switch len(matches) {
			case 1:
				if matches[0].File == h.File {
					entries = append(entries, migrationReconcileEntry{
						row:    h,
						status: MigrationHistoryConsistent,
						newID:  h.Identity,
					})
					continue
				}

				entries = append(entries, migrationReconcileEntry{
					row:     h,
					status:  MigrationHistoryRenamed,
					newName: matches[0].File,
					newID:   h.Identity,
				})
				continue
			default: // > 1
				names := make([]string, 0, len(matches))
				for _, m := range matches {
					names = append(names, m.File)
				}
				sort.Strings(names)
				undeterminable = append(undeterminable, fmt.Sprintf(
					"- applied migration %q content identity matches %d current migration sources (%s); unable to determine which one it corresponds to",
					h.File, len(matches), strings.Join(names, ", "),
				))
				continue
			case 0:
				// fall through to the name-based classification below
			}

			if sName == nil {
				// neither the old name nor the applied content can be found
				entries = append(entries, migrationReconcileEntry{
					row:    h,
					status: MigrationHistoryMissing,
				})
				continue
			}

			if sName.Identity != "" && sName.Identity != h.Identity {
				// same migration slot, but its content was edited after it was applied
				entries = append(entries, migrationReconcileEntry{
					row:    h,
					status: MigrationHistoryContentChanged,
					newID:  sName.Identity,
				})
				continue
			}

			// the current source doesn't expose an identity, so there is no proof of change
			entries = append(entries, migrationReconcileEntry{
				row:    h,
				status: MigrationHistoryConsistent,
				newID:  h.Identity,
			})
			continue
		}

		// legacy entry without recorded identity
		if sName != nil {
			entries = append(entries, migrationReconcileEntry{
				row:    h,
				status: MigrationHistoryConsistent,
				newID:  sName.Identity, // may be empty; used to backfill the identity column
			})
			continue
		}

		// defer: could be a rename or a genuinely missing source - decide below
		// once all rename targets are known
		entries = append(entries, migrationReconcileEntry{
			row:    h,
			status: "", // unresolved legacy entry
		})
	}

	// validate rename targets and collect the final set of history file names
	renames := map[string]migrationReconcileEntry{} // old file -> entry
	renameTargets := map[string]string{}            // new file -> old file
	finalNames := map[string]struct{}{}

	for _, entry := range entries {
		switch entry.status {
		case MigrationHistoryRenamed:
			if from, ok := renameTargets[entry.newName]; ok {
				undeterminable = append(undeterminable, fmt.Sprintf(
					"- both applied migrations %q and %q appear to have been renamed to %q; unable to determine the correct mapping",
					from, entry.row.File, entry.newName,
				))
				continue
			}
			renames[entry.row.File] = entry
			renameTargets[entry.newName] = entry.row.File
			finalNames[entry.newName] = struct{}{}
		case "":
			// handled later
		default:
			finalNames[entry.row.File] = struct{}{}
		}
	}

	renamingAway := map[string]struct{}{}
	for from := range renames {
		renamingAway[from] = struct{}{}
	}

	for to, from := range renameTargets {
		if occupant, ok := historyByName[to]; ok {
			if _, movingAway := renamingAway[occupant.File]; !movingAway {
				undeterminable = append(undeterminable, fmt.Sprintf(
					"- applied migration %q appears to have been renamed to %q, but %q is already applied as a separate migration; unable to determine the correct mapping",
					from, to, occupant.File,
				))
			}
		}
	}

	// current sources that are not part of the post-reconciliation history
	// (they are the candidates for legacy entries with missing content identity)
	candidates := []string{}
	for _, m := range sources {
		if _, ok := finalNames[m.File]; !ok {
			candidates = append(candidates, m.File)
		}
	}
	sort.Strings(candidates)

	for i := range entries {
		if entries[i].status != "" {
			continue
		}

		if len(candidates) == 0 {
			// nothing it could plausibly correspond to - the source really disappeared
			entries[i].status = MigrationHistoryMissing
			continue
		}

		undeterminable = append(undeterminable, fmt.Sprintf(
			"- applied migration %q has no recorded content identity and its source is missing; it could correspond to the current migration(s) %s, but it cannot be determined whether their content matches (rename) or changed - please resolve manually",
			entries[i].row.File, strings.Join(candidates, ", "),
		))
	}

	if len(undeterminable) > 0 {
		sort.Strings(undeterminable)
		return nil, fmt.Errorf("%w:\n%s", ErrUndeterminableMigrationsHistory, strings.Join(undeterminable, "\n"))
	}

	// everything is determined - converge within the current transaction
	//
	// note: renames are performed in 2 steps using a temporary file name
	// to support cyclic renames (eg. a <-> b) without violating the PK constraint
	renameIndex := 0
	renameMarkers := map[string]string{} // old file -> marker

	for _, entry := range renames {
		marker := fmt.Sprintf("%s%04d_%s", reconcileTempPrefix, renameIndex, entry.row.File)
		renameIndex++
		renameMarkers[entry.row.File] = marker

		if _, err := txApp.DB().Update(
			r.tableName,
			dbx.Params{"file": marker},
			dbx.HashExp{"file": entry.row.File},
		).Execute(); err != nil {
			return nil, err
		}
	}

	result := &ReconcileMigrationsResult{Items: make([]ReconciledMigration, 0, len(entries))}

	for _, entry := range entries {
		switch entry.status {
		case MigrationHistoryRenamed:
			marker := renameMarkers[entry.row.File]
			if _, err := txApp.DB().Update(
				r.tableName,
				dbx.Params{"file": entry.newName, "identity": entry.newID},
				dbx.HashExp{"file": marker},
			).Execute(); err != nil {
				return nil, err
			}

			result.Items = append(result.Items, ReconciledMigration{
				File:     entry.row.File,
				NewFile:  entry.newName,
				Identity: entry.newID,
				Status:   MigrationHistoryRenamed,
			})
		case MigrationHistoryContentChanged:
			if _, err := txApp.DB().Update(
				r.tableName,
				dbx.Params{"identity": entry.newID},
				dbx.HashExp{"file": entry.row.File},
			).Execute(); err != nil {
				return nil, err
			}

			result.Items = append(result.Items, ReconciledMigration{
				File:     entry.row.File,
				Identity: entry.newID,
				Status:   MigrationHistoryContentChanged,
			})
		case MigrationHistoryMissing:
			if _, err := txApp.DB().Delete(
				r.tableName,
				dbx.HashExp{"file": entry.row.File},
			).Execute(); err != nil {
				return nil, err
			}

			result.Items = append(result.Items, ReconciledMigration{
				File:   entry.row.File,
				Status: MigrationHistoryMissing,
			})
		default: // consistent (optionally with identity backfill)
			if entry.newID != "" && entry.newID != entry.row.Identity {
				if _, err := txApp.DB().Update(
					r.tableName,
					dbx.Params{"identity": entry.newID},
					dbx.HashExp{"file": entry.row.File},
				).Execute(); err != nil {
					return nil, err
				}
			}

			result.Items = append(result.Items, ReconciledMigration{
				File:     entry.row.File,
				Identity: entry.newID,
				Status:   MigrationHistoryConsistent,
			})
		}
	}

	return result, nil
}
