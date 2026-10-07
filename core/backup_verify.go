package core

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pocketbase/pocketbase/tools/archive"
	"github.com/pocketbase/pocketbase/tools/filesystem"
	"github.com/pocketbase/pocketbase/tools/types"
)

const (
	// StoreKeyActiveBackupVerification is used to ensure that only a single
	// backup reconciliation/verification process can run at a time.
	StoreKeyActiveBackupVerification = "@activeBackupVerification"

	// BackupVerificationManifestKey is the reserved key in the backups
	// filesystem under which the persisted verification results are stored.
	//
	// The manifest lives next to the backups so that it works with both local
	// and S3 storage and travels together with the backup files.
	// It is not a regular backup (doesn't have .zip suffix) and is excluded
	// from the backups listing and retention operations.
	BackupVerificationManifestKey = "@backup_verification.json"

	backupVerificationManifestTmpSuffix = ".tmp"
	backupVerificationManifestVersion   = 1
)

// BackupVerificationStatus represents the verification status of a single backup.
type BackupVerificationStatus string

const (
	// BackupVerificationValid means that the backup archive passed the full
	// integrity verification and can be safely restored.
	BackupVerificationValid BackupVerificationStatus = "valid"

	// BackupVerificationInvalid means that the backup archive is corrupt,
	// incomplete or couldn't be verified at all.
	//
	// Such backups are never deleted automatically and are kept and reported
	// separately for manual inspection.
	BackupVerificationInvalid BackupVerificationStatus = "invalid"
)

// BackupVerificationEntry is a single backup verification result.
type BackupVerificationEntry struct {
	Key       string                   `json:"key"`
	Size      int64                    `json:"size"`
	Modified  types.DateTime           `json:"modified"`
	Status    BackupVerificationStatus `json:"status"`
	Error     string                   `json:"error,omitempty"`
	CheckedAt types.DateTime           `json:"checkedAt"`

	// Reused indicates whether the result was reused from a previous run
	// (aka. the backup hasn't changed and it wasn't re-verified).
	Reused bool `json:"reused"`
}

// BackupVerificationReport holds the verification state of all backup
// candidates after a [App.ReconcileBackups] run.
type BackupVerificationReport struct {
	Entries []BackupVerificationEntry `json:"entries"`
}

// Find returns the verification entry for the specified backup key.
func (r *BackupVerificationReport) Find(key string) (BackupVerificationEntry, bool) {
	if r == nil {
		return BackupVerificationEntry{}, false
	}

	for _, entry := range r.Entries {
		if entry.Key == key {
			return entry, true
		}
	}

	return BackupVerificationEntry{}, false
}

// Valid returns all entries that passed the verification.
func (r *BackupVerificationReport) Valid() []BackupVerificationEntry {
	return r.filterByStatus(BackupVerificationValid)
}

// Invalid returns all entries that failed the verification or couldn't be
// verified, together with the failure reason.
func (r *BackupVerificationReport) Invalid() []BackupVerificationEntry {
	return r.filterByStatus(BackupVerificationInvalid)
}

func (r *BackupVerificationReport) filterByStatus(status BackupVerificationStatus) []BackupVerificationEntry {
	if r == nil {
		return nil
	}

	result := []BackupVerificationEntry{}

	for _, entry := range r.Entries {
		if entry.Status == status {
			result = append(result, entry)
		}
	}

	return result
}

// backupVerificationRecord is a single persisted backup verification result.
type backupVerificationRecord struct {
	Size      int64     `json:"size"`
	Modified  time.Time `json:"modified"`
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
	CheckedAt time.Time `json:"checkedAt"`
}

func (r backupVerificationRecord) matches(size int64, modTime time.Time) bool {
	return r.Size == size && r.Modified.Equal(modTime) &&
		(r.Status == string(BackupVerificationValid) || r.Status == string(BackupVerificationInvalid))
}

// backupVerificationManifest is the persisted verification state container.
type backupVerificationManifest struct {
	Version int                                 `json:"version"`
	Records map[string]backupVerificationRecord `json:"records"`
}

func newBackupVerificationManifest() *backupVerificationManifest {
	return &backupVerificationManifest{
		Version: backupVerificationManifestVersion,
		Records: map[string]backupVerificationRecord{},
	}
}

// ReconcileBackups performs a full integrity reconciliation of all candidate
// backup files in the configured backups storage (local or S3).
//
// Each backup archive is verified end-to-end (zip structure, CRC-32 of every
// entry and presence of a valid data.db file).
//
// The results are persisted in the backups filesystem so that unchanged
// backups are not re-verified on subsequent runs. The manifest is updated
// after each processed backup, allowing interrupted runs to resume from
// where they left off (only pending/changed backups being processed).
//
// Corrupt or unverifiable backups are reported in the result but are never
// deleted automatically - only backups marked as [BackupVerificationValid]
// are allowed to participate in the retention quota calculations.
func (app *BaseApp) ReconcileBackups(ctx context.Context) (*BackupVerificationReport, error) {
	if app.Store().Has(StoreKeyActiveBackupVerification) {
		return nil, errors.New("try again later - another backup verification process has already been started")
	}

	app.Store().Set(StoreKeyActiveBackupVerification, struct{}{})
	defer app.Store().Remove(StoreKeyActiveBackupVerification)

	startTime := time.Now()

	fsys, err := app.NewBackupsFilesystem()
	if err != nil {
		return nil, err
	}
	defer fsys.Close()

	fsys.SetContext(ctx)

	files, err := fsys.List("")
	if err != nil {
		return nil, fmt.Errorf("failed to list backup files: %w", err)
	}

	candidates := make([]*filesystemListObject, 0, len(files))
	present := make(map[string]struct{}, len(files))
	for _, f := range files {
		if !isBackupVerificationCandidate(f.Key) {
			continue
		}
		candidates = append(candidates, &filesystemListObject{Key: f.Key, Size: f.Size, ModTime: f.ModTime})
		present[f.Key] = struct{}{}
	}

	// deterministic order so that interrupted runs resume in a predictable way
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Key < candidates[j].Key
	})

	manifest := app.loadBackupVerificationManifest(fsys)

	// drop records for backups that no longer exist
	for key := range manifest.Records {
		if _, ok := present[key]; !ok {
			delete(manifest.Records, key)
		}
	}

	report := &BackupVerificationReport{Entries: []BackupVerificationEntry{}}

	interrupted := false

	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			interrupted = true
			break
		}

		key := candidate.Key

		// skip backups that are being written at the moment to avoid
		// falsely labeling a half-uploaded archive
		if active, _ := app.Store().Get(StoreKeyActiveBackup).(string); active == key {
			continue
		}

		// reuse the persisted result if the backup hasn't changed
		if cached, ok := manifest.Records[key]; ok && cached.matches(candidate.Size, candidate.ModTime) {
			report.Entries = append(report.Entries, backupVerificationEntryFromRecord(key, cached, true))
			continue
		}

		status, verifyErrMsg, conclusive := app.verifyBackupObject(ctx, fsys, key, candidate.Size)

		// don't persist anything if the verification process itself was
		// interrupted - the backup should be retried on the next run
		if ctx.Err() != nil {
			interrupted = true
			break
		}

		record := backupVerificationRecord{
			Size:      candidate.Size,
			Modified:  candidate.ModTime,
			Status:    string(status),
			Error:     verifyErrMsg,
			CheckedAt: time.Now().UTC(),
		}

		entry := backupVerificationEntryFromRecord(key, record, false)

		if status == BackupVerificationInvalid {
			app.Logger().Warn(
				"[Backup verify] Corrupt, incomplete or unverifiable backup detected",
				slog.String("key", key),
				slog.String("error", verifyErrMsg),
			)
		}

		report.Entries = append(report.Entries, entry)

		if !conclusive {
			// the backup couldn't be actually verified (eg. transient
			// storage/download error) - report it for this run but don't
			// persist the result so that it gets retried on the next run
			continue
		}

		manifest.Records[key] = record

		// persist after each processed backup so that interrupted runs
		// can resume without redoing the completed work
		if err := app.saveBackupVerificationManifest(fsys, manifest); err != nil {
			app.Logger().Warn(
				"[Backup verify] Failed to persist verification manifest",
				slog.String("error", err.Error()),
			)
		}
	}

	// always persist the manifest even if the run was interrupted
	// (use a detached context since the caller's one may be already canceled)
	fsys.SetContext(context.Background())
	if err := app.saveBackupVerificationManifest(fsys, manifest); err != nil {
		app.Logger().Warn(
			"[Backup verify] Failed to persist verification manifest",
			slog.String("error", err.Error()),
		)
	}

	sort.Slice(report.Entries, func(i, j int) bool {
		return report.Entries[i].Key < report.Entries[j].Key
	})

	app.Logger().Debug(
		"[Backup verify] Reconciliation completed",
		slog.Int("total", len(report.Entries)),
		slog.Int("valid", len(report.Valid())),
		slog.Int("invalid", len(report.Invalid())),
		slog.Bool("interrupted", interrupted),
		slog.Float64("execTime", float64(time.Since(startTime))/float64(time.Millisecond)),
	)

	if interrupted {
		return report, ctx.Err()
	}

	return report, nil
}

// FindBackupVerificationEntries returns the persisted verification entries
// from the last completed (or partially completed) reconciliation.
//
// Be aware that the list may contain records for backups that were deleted
// after the last reconciliation; use the keys from the backups listing as
// source of truth which backups currently exist.
func (app *BaseApp) FindBackupVerificationEntries() ([]BackupVerificationEntry, error) {
	fsys, err := app.NewBackupsFilesystem()
	if err != nil {
		return nil, err
	}
	defer fsys.Close()

	fsys.SetContext(context.Background())

	manifest := app.loadBackupVerificationManifest(fsys)

	keys := make([]string, 0, len(manifest.Records))
	for key := range manifest.Records {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]BackupVerificationEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, backupVerificationEntryFromRecord(key, manifest.Records[key], true))
	}

	return entries, nil
}

// filesystemListObject is a lightweight subset of [blob.ListObject]
// used within the verification process.
type filesystemListObject struct {
	Key     string
	Size    int64
	ModTime time.Time
}

func isBackupVerificationCandidate(key string) bool {
	return !IsReservedBackupKey(key) && strings.HasSuffix(key, ".zip")
}

// IsReservedBackupKey reports whether the specified backups filesystem key
// is reserved for the backup verification feature internals (the persisted
// manifest or its temp replacement) and shouldn't be treated as a backup.
func IsReservedBackupKey(key string) bool {
	return key == BackupVerificationManifestKey ||
		strings.HasSuffix(key, backupVerificationManifestTmpSuffix)
}

func backupVerificationEntryFromRecord(key string, record backupVerificationRecord, reused bool) BackupVerificationEntry {
	modified, _ := types.ParseDateTime(record.Modified)
	checkedAt, _ := types.ParseDateTime(record.CheckedAt)

	status := BackupVerificationStatus(record.Status)
	if status != BackupVerificationValid && status != BackupVerificationInvalid {
		status = BackupVerificationInvalid
	}

	return BackupVerificationEntry{
		Key:       key,
		Size:      record.Size,
		Modified:  modified,
		Status:    status,
		Error:     record.Error,
		CheckedAt: checkedAt,
		Reused:    reused,
	}
}

// verifyBackupObject verifies a single backup object.
//
// It returns:
//   - status: the verification status (valid or invalid);
//   - errMsg: a human readable failure reason (empty when valid);
//   - conclusive: false when the backup couldn't be actually verified
//     (eg. a transient storage/download error), meaning that the result
//     shouldn't be persisted and the backup should be retried later.
func (app *BaseApp) verifyBackupObject(ctx context.Context, fsys *filesystem.System, key string, expectedSize int64) (status BackupVerificationStatus, errMsg string, conclusive bool) {
	inconclusive := func(err error) (BackupVerificationStatus, string, bool) {
		return BackupVerificationInvalid, err.Error(), false
	}
	invalid := func(err error) (BackupVerificationStatus, string, bool) {
		return BackupVerificationInvalid, err.Error(), true
	}

	localPath, cleanup, err := app.materializeBackupForVerification(ctx, fsys, key)
	if err != nil {
		return inconclusive(err)
	}
	defer cleanup()

	// ensure that the whole object was fetched (helps detecting truncated uploads)
	if expectedSize > 0 {
		info, statErr := os.Stat(localPath)
		if statErr != nil {
			return inconclusive(fmt.Errorf("failed to stat the downloaded backup: %w", statErr))
		}
		if info.Size() != expectedSize {
			return inconclusive(fmt.Errorf("backup size mismatch: expected %d bytes but got %d", expectedSize, info.Size()))
		}
	}

	// full zip integrity check (central directory + CRC-32 of all entries)
	if err := archive.Verify(localPath, dataDBFilename); err != nil {
		return invalid(fmt.Errorf("archive integrity check failed: %w", err))
	}

	// ensure that the main database is a valid SQLite database
	if err := verifyBackupSQLiteDatabase(localPath); err != nil {
		return invalid(err)
	}

	return BackupVerificationValid, "", true
}

// materializeBackupForVerification returns a local filesystem path to a zip
// file that can be opened with [zip.OpenReader].
//
// For local backups storage it returns the direct backup path, while for S3
// the object is downloaded to a temp file inside pb_data.
// The returned cleanup function should be always called.
func (app *BaseApp) materializeBackupForVerification(ctx context.Context, fsys *filesystem.System, key string) (string, func(), error) {
	if !app.Settings().Backups.S3.Enabled {
		// same as in restore - construct manually the local path to avoid creating a copy
		localPath := filepath.Join(app.DataDir(), LocalBackupsDirName, filepath.Base(key))

		info, err := os.Stat(localPath)
		if err != nil {
			return "", nil, fmt.Errorf("failed to access local backup %q: %w", key, err)
		}
		if info.IsDir() {
			return "", nil, errIsDir
		}

		return localPath, func() {}, nil
	}

	localTempDir := filepath.Join(app.DataDir(), LocalTempDirName)
	if err := os.MkdirAll(localTempDir, os.ModePerm); err != nil {
		return "", nil, fmt.Errorf("failed to create a temp dir: %w", err)
	}

	br, err := fsys.GetReader(key)
	if err != nil {
		return "", nil, fmt.Errorf("failed to open backup %q: %w", key, err)
	}
	defer br.Close()

	tempZip, err := os.CreateTemp(localTempDir, "pb_verify_*.zip")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create a temp backup file: %w", err)
	}
	tempPath := tempZip.Name()

	cleanup := func() {
		_ = os.Remove(tempPath)
	}

	if _, err := io.Copy(tempZip, br); err != nil {
		_ = tempZip.Close()
		cleanup()
		return "", nil, fmt.Errorf("failed to download backup %q: %w", key, err)
	}

	if err := tempZip.Close(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to finalize the temp backup file: %w", err)
	}

	return tempPath, cleanup, nil
}

// verifyBackupSQLiteDatabase checks that the required data.db entry within
// the verified zip archive starts with the SQLite database header.
//
// (archive.Verify already ensured that data.db exists as a regular file with
// valid CRC-32 checksum)
func verifyBackupSQLiteDatabase(zipPath string) error {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer zr.Close()

	for _, f := range zr.File {
		if f.Name != dataDBFilename {
			continue
		}

		if f.FileInfo().IsDir() || !f.FileInfo().Mode().IsRegular() {
			break
		}

		r, err := f.Open()
		if err != nil {
			return fmt.Errorf("failed to open %s from the backup archive: %w", dataDBFilename, err)
		}
		defer r.Close()

		header := make([]byte, len(sqliteDatabaseHeader))
		if _, err := io.ReadFull(r, header); err != nil {
			return fmt.Errorf("failed to read the %s header: %w", dataDBFilename, err)
		}

		if !bytes.Equal(header, sqliteDatabaseHeader) {
			return fmt.Errorf("%s is not a valid SQLite database (missing database header)", dataDBFilename)
		}

		return nil
	}

	return fmt.Errorf("%s is missing or invalid", dataDBFilename)
}

// sqliteDatabaseHeader is the 16-byte magic string at the start of every
// SQLite database file (see https://www.sqlite.org/fileformat2.html#magic).
var sqliteDatabaseHeader = []byte("SQLite format 3\x00")

func (app *BaseApp) loadBackupVerificationManifest(fsys *filesystem.System) *backupVerificationManifest {
	empty := newBackupVerificationManifest()

	r, err := fsys.GetReader(BackupVerificationManifestKey)
	if err != nil {
		// doesn't exist yet (or the storage is eventually consistent)
		return empty
	}
	defer r.Close()

	raw, err := io.ReadAll(r)
	if err != nil {
		app.Logger().Warn(
			"[Backup verify] Failed to read the persisted verification manifest; starting with empty state",
			slog.String("error", err.Error()),
		)
		return empty
	}

	manifest := newBackupVerificationManifest()
	if err := json.Unmarshal(raw, manifest); err != nil {
		app.Logger().Warn(
			"[Backup verify] The persisted verification manifest is invalid; starting with empty state",
			slog.String("error", err.Error()),
		)
		return newBackupVerificationManifest()
	}

	if manifest.Records == nil {
		manifest.Records = map[string]backupVerificationRecord{}
	}

	return manifest
}

// saveBackupVerificationManifest persists the manifest atomically by first
// uploading to a temp key and then replacing the primary key.
func (app *BaseApp) saveBackupVerificationManifest(fsys *filesystem.System, manifest *backupVerificationManifest) error {
	manifest.Version = backupVerificationManifestVersion

	raw, err := json.MarshalIndent(manifest, "", " ")
	if err != nil {
		return err
	}

	tmpKey := BackupVerificationManifestKey + backupVerificationManifestTmpSuffix

	if err := fsys.Upload(raw, tmpKey); err != nil {
		return fmt.Errorf("failed to upload temp manifest: %w", err)
	}

	if err := fsys.Copy(tmpKey, BackupVerificationManifestKey); err != nil {
		_ = fsys.Delete(tmpKey)
		return fmt.Errorf("failed to replace the verification manifest: %w", err)
	}

	if err := fsys.Delete(tmpKey); err != nil {
		// non-critical - the stale temp key will be overwritten on the next save
		app.Logger().Warn(
			"[Backup verify] Failed to remove the temp verification manifest",
			slog.String("error", err.Error()),
		)
	}

	return nil
}
