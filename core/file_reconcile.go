package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase/tools/filesystem"
	"github.com/pocketbase/pocketbase/tools/list"
	"github.com/pocketbase/pocketbase/tools/types"
	"github.com/spf13/cast"
)

// DefaultFileReconcileOrphanGracePeriod is the default grace period used
// by [App.ReconcileFiles] for freshly registered managed files.
//
// It exists to minimize the risk of deleting a file that was just committed
// but wasn't yet picked by the references snapshot during a concurrent run.
const DefaultFileReconcileOrphanGracePeriod = 2 * time.Minute

// FileReference describes a single file reference extracted from a record.
type FileReference struct {
	// Collection is the id of the collection the reference belongs to.
	Collection string `json:"collection"`

	// Record is the id of the record the reference belongs to.
	Record string `json:"record"`

	// Field is the name of the file field holding the reference.
	Field string `json:"field"`

	// FileKey is the full storage key of the referenced file.
	FileKey string `json:"fileKey"`

	// Name is the plain referenced file name.
	Name string `json:"name"`
}

// FileContentIssue describes a referenced file whose storage state
// doesn't match its ownership record.
type FileContentIssue struct {
	// Reference is the record reference that points to the file.
	Reference FileReference `json:"reference"`

	// Ownership is the persisted ownership record of the file.
	Ownership *FileOwnership `json:"ownership"`

	// StoredDigest is the actual sha256 digest computed from the storage content.
	StoredDigest string `json:"storedDigest"`

	// StoredSize is the actual stored file size.
	StoredSize int64 `json:"storedSize"`

	// Reason is a short human readable reason for the issue
	// (currently only FileReconcileIssueModified).
	Reason string `json:"reason"`
}

const (
	// FileReconcileIssueModified indicates that a referenced file content
	// no longer matches the ownership digest.
	FileReconcileIssueModified = "modified"
)

// FileReconcileOptions defines the options for a single [App.ReconcileFiles] run.
type FileReconcileOptions struct {
	// DryRun when enabled performs only the analysis/reporting without
	// writing to the ownership records or deleting anything from the storage.
	DryRun bool

	// DeleteOrphans controls whether managed files that are confirmed to have
	// no record reference will be deleted (alongside their derived thumbs).
	//
	// By default it is enabled.
	DeleteOrphans *bool

	// OrphanGracePeriod skips managed files whose ownership records were
	// created within the last specified duration to avoid deleting files
	// that may be part of an in-flight concurrent record update.
	//
	// When zero it fallsbacks to DefaultFileReconcileOrphanGracePeriod.
	//
	// Set a negative value to disable.
	OrphanGracePeriod time.Duration
}

// FileReconcileReport holds the result of a single [App.ReconcileFiles] run.
type FileReconcileReport struct {
	// Verified lists referenced managed files whose content matched their ownership digest.
	Verified []*FileOwnership `json:"verified"`

	// Adopted lists storage files that are referenced by records but had no
	// ownership record and were registered (adopted) as managed during the run.
	Adopted []*FileOwnership `json:"adopted"`

	// Missing lists referenced files that are not present in the storage.
	Missing []*FileReference `json:"missing"`

	// Modified lists referenced files whose content no longer matches their
	// ownership digest. The files are left untouched.
	Modified []*FileContentIssue `json:"modified"`

	// Orphans lists managed files (and in a non dry-run - already deleted ones)
	// that were confirmed to have no record reference.
	Orphans []*FileOwnership `json:"orphans"`

	// Pending lists otherwise orphan-looking managed files that were skipped
	// because they are still within the configured grace period.
	Pending []*FileOwnership `json:"pending"`

	// External lists storage keys that have neither a record reference nor an
	// ownership record. They are treated as externally managed and are left untouched.
	External []string `json:"external"`

	// Errors contains non-fatal per-item errors encountered during the run
	// (eg. a file that couldn't be read for digest verification or a failed orphan delete).
	Errors []error `json:"-"`
}

// HasErrors indicates whether the run produced non-fatal errors.
func (r *FileReconcileReport) HasErrors() bool {
	return len(r.Errors) > 0
}

// ReconcileFiles performs a storage reconciliation using the record file
// references as source of truth.
//
// The reconciliation is fully idempotent and resumable - rerunning it after
// a failure or a crash will continue converging toward the desired state
// because every managed file persists its own [FileOwnership] record and
// deletions happen per-file (storage object first, ownership record last).
//
// For safety the method doesn't perform ANY delete/write if it can't read
// the storage, enumerate the references or load the ownership records,
// because in such case it is impossible to make a safe delete decision.
//
// See [FileReconcileOptions] for the available run options.
func (app *BaseApp) ReconcileFiles(ctx context.Context, options ...*FileReconcileOptions) (*FileReconcileReport, error) {
	report := &FileReconcileReport{
		Verified: []*FileOwnership{},
		Adopted:  []*FileOwnership{},
		Missing:  []*FileReference{},
		Modified: []*FileContentIssue{},
		Orphans:  []*FileOwnership{},
		Pending:  []*FileOwnership{},
		External: []string{},
	}

	opt := &FileReconcileOptions{}
	if len(options) > 0 && options[0] != nil {
		opt = options[0]
	}

	deleteOrphans := true
	if opt.DeleteOrphans != nil {
		deleteOrphans = *opt.DeleteOrphans
	}

	grace := opt.OrphanGracePeriod
	if grace == 0 {
		grace = DefaultFileReconcileOrphanGracePeriod
	}

	startedAt := time.Now()

	// 1. Ensure that the ownership table exists so that we can distinguish
	// managed from external files (fail early without guessing).
	if !app.AuxHasTable(FileOwnershipTableName) {
		return report, fmt.Errorf("missing %q table - please run the application migrations first", FileOwnershipTableName)
	}

	ownerships, err := app.FindAllFileOwnerships()
	if err != nil {
		return report, fmt.Errorf("failed to load file ownership records: %w", err)
	}
	ownershipByKey := make(map[string]*FileOwnership, len(ownerships))
	for _, m := range ownerships {
		ownershipByKey[m.FileKey] = m
	}

	// 2. Enumerate the record references (source of truth).
	references, err := app.collectFileReferences()
	if err != nil {
		return report, err
	}

	// 3. Snapshot the storage - no deletes are allowed if we can't list it.
	fsys, err := app.NewFilesystem()
	if err != nil {
		return report, fmt.Errorf("failed to initialize filesystems: %w", err)
	}
	defer fsys.Close()
	fsys.SetContext(ctx)

	objects, err := fsys.List("")
	if err != nil {
		return report, fmt.Errorf("failed to list storage files (reconcile aborted to prevent unsafe deletes): %w", err)
	}

	presentKeys := map[string]struct{}{}
	thumbKeysByOriginal := map[string][]string{}
	originalKeys := make([]string, 0, len(objects))

	for _, obj := range objects {
		if obj.IsDir {
			continue
		}

		key := obj.Key
		if key == "" || strings.HasSuffix(key, ".attrs") {
			continue // driver sidecar metadata or invalid
		}

		presentKeys[key] = struct{}{}

		if originalKey, ok := originalFileKeyFromThumbsKey(key); ok {
			thumbKeysByOriginal[originalKey] = append(thumbKeysByOriginal[originalKey], key)
		} else {
			originalKeys = append(originalKeys, key)
		}
	}

	// 4. Verify referenced files and adopt missing ownership records.
	refKeys := make([]string, 0, len(references))
	for key := range references {
		refKeys = append(refKeys, key)
	}
	sort.Strings(refKeys)

	for _, key := range refKeys {
		if err := ctx.Err(); err != nil {
			return report, err
		}

		ref := references[key]

		if _, ok := presentKeys[key]; !ok {
			// referenced but missing in the storage (never auto-delete anything)
			refCopy := ref
			report.Missing = append(report.Missing, &refCopy)
			continue
		}

		digest, size, hashErr := hashStoredFile(ctx, fsys, key)
		if hashErr != nil {
			report.Errors = append(report.Errors, fmt.Errorf("verify %q: %w", key, hashErr))
			continue // can't verify - leave as-is and rely on another run
		}

		knownThumbs := list.ToUniqueStringSlice(thumbKeysByOriginal[key])
		sort.Strings(knownThumbs)

		existing, hasOwnership := ownershipByKey[key]

		// content drift detection (only when we have a previous digest to compare with)
		if hasOwnership && existing.Digest != "" && existing.Digest != digest {
			report.Modified = append(report.Modified, &FileContentIssue{
				Reference:    ref,
				Ownership:    existing,
				StoredDigest: digest,
				StoredSize:   size,
				Reason:       FileReconcileIssueModified,
			})
			continue // do not touch the file or auto-update the digest
		}

		if !hasOwnership {
			// referenced but previously untracked (eg. pre-feature file or
			// ownership row that was lost) - adopt it as managed
			now := types.NowDateTime()

			m := &FileOwnership{
				Collection: ref.Collection,
				Record:     ref.Record,
				Field:      ref.Field,
				FileKey:    key,
				Name:       ref.Name,
				Digest:     digest,
				Size:       size,
				Thumbs:     knownThumbs,
				Created:    now,
				Updated:    now,
			}
			m.Id = generateFileOwnershipId(key)
			m.MarkAsNew()

			if !opt.DryRun {
				if err := app.upsertFileOwnership(m); err != nil {
					report.Errors = append(report.Errors, fmt.Errorf("adopt %q: %w", key, err))
					continue
				}
			}
			m.MarkAsNotNew()

			report.Adopted = append(report.Adopted, m)
			ownershipByKey[key] = m

			continue
		}

		// healthy referenced managed file - just keep its metadata fresh
		changed := false
		if existing.Collection != ref.Collection || existing.Record != ref.Record || existing.Field != ref.Field {
			existing.Collection = ref.Collection
			existing.Record = ref.Record
			existing.Field = ref.Field
			changed = true
		}
		if existing.Name != ref.Name {
			existing.Name = ref.Name
			changed = true
		}
		if existing.Digest != digest {
			existing.Digest = digest
			changed = true
		}
		if existing.Size != size {
			existing.Size = size
			changed = true
		}
		existingThumbs := list.ToUniqueStringSlice(existing.Thumbs)
		sort.Strings(existingThumbs)
		if !equalStringSlices(existingThumbs, knownThumbs) {
			existing.Thumbs = knownThumbs
			changed = true
		}

		if changed {
			existing.Updated = types.NowDateTime()
			if !opt.DryRun {
				if err := app.upsertFileOwnership(existing); err != nil {
					report.Errors = append(report.Errors, fmt.Errorf("update %q: %w", key, err))
					continue
				}
			}
		}

		report.Verified = append(report.Verified, existing)
	}

	// 5. Classify the remaining ownership records (unreferenced managed files).
	orphanCandidates := make([]*FileOwnership, 0)
	for _, m := range ownershipByKey {
		if _, referenced := references[m.FileKey]; referenced {
			continue
		}
		orphanCandidates = append(orphanCandidates, m)
	}
	sort.Slice(orphanCandidates, func(i, j int) bool {
		return orphanCandidates[i].FileKey < orphanCandidates[j].FileKey
	})

	cutoff := startedAt.Add(-grace)

	for _, m := range orphanCandidates {
		if err := ctx.Err(); err != nil {
			return report, err
		}

		if grace > 0 && !m.Created.Time().IsZero() && m.Created.Time().After(cutoff) {
			// too new - there may be an in-flight reference for it
			report.Pending = append(report.Pending, m)
			continue
		}

		if !opt.DryRun && deleteOrphans {
			if err := app.deleteOrphanedFileOwnership(ctx, fsys, m); err != nil {
				report.Errors = append(report.Errors, err)
				continue // keep the ownership row so the next run can retry
			}
		}

		report.Orphans = append(report.Orphans, m)
	}

	// 6. Everything else in the storage is external and must remain untouched.
	// (includes unreferenced files without ownership and their stray thumbs)
	for _, key := range originalKeys {
		if _, referenced := references[key]; referenced {
			continue
		}
		if _, managed := ownershipByKey[key]; managed {
			continue
		}
		report.External = append(report.External, key)
	}
	for original, thumbs := range thumbKeysByOriginal {
		if _, referenced := references[original]; referenced {
			continue
		}
		if _, managed := ownershipByKey[original]; managed {
			continue
		}
		report.External = append(report.External, thumbs...)
	}
	sort.Strings(report.External)

	return report, nil
}

// deleteOrphanedFileOwnership deletes an unreferenced managed file and its
// derived thumbs, removing the ownership record only AFTER the storage
// operations have succeeded.
//
// Missing storage objects are tolerated so the method is safe to retry
// after a crash in the middle of a previous run.
func (app *BaseApp) deleteOrphanedFileOwnership(ctx context.Context, fsys *filesystem.System, m *FileOwnership) error {
	// thumbs (the prefix delete also catches thumbs created after the last run)
	if errs := fsys.DeletePrefix(m.ThumbsPrefix()); len(errs) > 0 {
		return fmt.Errorf("orphan %q: failed to delete thumbs: %w", m.FileKey, errors.Join(errs...))
	}

	// original (ErrNotFound is ok - it may have been deleted before a crash)
	if err := fsys.Delete(m.FileKey); err != nil && !errors.Is(err, filesystem.ErrNotFound) {
		return fmt.Errorf("orphan %q: failed to delete file: %w", m.FileKey, err)
	}

	// ownership row last (acts as the completion marker for a crash)
	if err := app.AuxDelete(m); err != nil {
		return fmt.Errorf("orphan %q: file was deleted but failed to remove its ownership record: %w", m.FileKey, err)
	}

	return nil
}

// collectFileReferences enumerates all file references from the base/auth
// collection records (view collections don't own storage files).
func (app *BaseApp) collectFileReferences() (map[string]FileReference, error) {
	collections, err := app.FindAllCollections(CollectionTypeBase, CollectionTypeAuth)
	if err != nil {
		return nil, fmt.Errorf("failed to enumerate collections: %w", err)
	}

	references := map[string]FileReference{}

	for _, collection := range collections {
		fileFields := []*FileField{}
		for _, f := range collection.Fields {
			if ff, ok := f.(*FileField); ok {
				fileFields = append(fileFields, ff)
			}
		}
		if len(fileFields) == 0 {
			continue // no point loading the records
		}

		records, err := app.FindAllRecords(collection)
		if err != nil {
			return nil, fmt.Errorf("failed to enumerate records for collection %q: %w", collection.Name, err)
		}

		for _, record := range records {
			recordId := cast.ToString(record.LastSavedPK())
			if recordId == "" {
				recordId = record.Id
			}

			for _, ff := range fileFields {
				for _, name := range list.ToUniqueStringSlice(record.Get(ff.Name)) {
					if name == "" || strings.ContainsAny(name, "/\\") {
						continue // empty or not a plain filename
					}

					key := record.BaseFilesPath() + "/" + name

					// keep the first reference (duplicates are possible when
					// the same file is assigned to multiple file fields)
					if _, exists := references[key]; exists {
						continue
					}

					references[key] = FileReference{
						Collection: collection.Id,
						Record:     recordId,
						Field:      ff.Name,
						FileKey:    key,
						Name:       name,
					}
				}
			}
		}
	}

	return references, nil
}

// hashStoredFile computes the sha256 digest and the size of a stored file.
func hashStoredFile(ctx context.Context, fsys *filesystem.System, fileKey string) (string, int64, error) {
	r, err := fsys.GetReader(fileKey)
	if err != nil {
		return "", 0, err
	}
	defer r.Close()

	h := sha256.New()

	size, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}

	if err := ctx.Err(); err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(h.Sum(nil)), size, nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
