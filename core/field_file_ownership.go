package core

import (
	"context"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/tools/list"
	"github.com/pocketbase/pocketbase/tools/types"
	"github.com/spf13/cast"
)

// registerFilesOwnership computes and persists ownership records for the
// provided newly uploaded record files.
//
// It is best-effort - failures are only logged because [BaseApp.ReconcileFiles]
// can always reconstruct missing ownership records from the record references.
func (f *FileField) registerFilesOwnership(ctx context.Context, app App, record *Record, filenames []string) {
	names := list.ToUniqueStringSlice(filenames)
	if len(names) == 0 {
		return
	}

	fsys, err := app.NewFilesystem()
	if err != nil {
		app.Logger().Warn("Failed to initialize filesystem to register file ownership", "error", err)
		return
	}
	defer fsys.Close()
	fsys.SetContext(ctx)

	recordId := cast.ToString(record.LastSavedPK())
	if recordId == "" {
		recordId = record.Id
	}

	for _, name := range names {
		if name == "" {
			continue
		}

		fileKey := record.BaseFilesPath() + "/" + name

		digest, size, hashErr := hashStoredFile(ctx, fsys, fileKey)
		if hashErr != nil {
			app.Logger().Warn("Failed to compute file ownership digest", "key", fileKey, "error", hashErr)
			continue
		}

		now := types.NowDateTime()

		_, upsertErr := app.AuxDB().NewQuery(`
			INSERT INTO {{_fileOwnership}}
				([[id]], [[collection]], [[record]], [[field]], [[fileKey]], [[name]], [[digest]], [[size]], [[thumbs]], [[created]], [[updated]])
			VALUES
				({:id}, {:collection}, {:record}, {:field}, {:fileKey}, {:name}, {:digest}, {:size}, {:thumbs}, {:created}, {:updated})
			ON CONFLICT([[fileKey]]) DO UPDATE SET
				[[collection]] = excluded.[[collection]],
				[[record]]     = excluded.[[record]],
				[[field]]      = excluded.[[field]],
				[[name]]       = excluded.[[name]],
				[[digest]]     = excluded.[[digest]],
				[[size]]       = excluded.[[size]],
				[[updated]]    = excluded.[[updated]]
		`).Bind(dbx.Params{
			"id":         generateFileOwnershipId(fileKey),
			"collection": record.Collection().Id,
			"record":     recordId,
			"field":      f.Name,
			"fileKey":    fileKey,
			"name":       name,
			"digest":     digest,
			"size":       size,
			"thumbs":     types.JSONArray[string]{},
			"created":    now,
			"updated":    now,
		}).Execute()
		if upsertErr != nil {
			app.Logger().Warn("Failed to register file ownership", "key", fileKey, "error", upsertErr)
		}
	}
}

// unregisterFilesOwnership removes the ownership records associated with the
// provided deleted file keys (missing rows are ignored).
//
// It is best-effort - dangling records are safely pruned by the next
// [BaseApp.ReconcileFiles] run.
func (f *FileField) unregisterFilesOwnership(app App, fileKeys []string) {
	keys := make([]any, 0, len(fileKeys))
	for _, key := range list.ToUniqueStringSlice(fileKeys) {
		if key != "" {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return
	}

	_, err := app.AuxDB().
		Delete(FileOwnershipTableName, dbx.In("fileKey", keys...)).
		Execute()
	if err != nil {
		app.Logger().Warn("Failed to unregister file ownership records", "keys", fileKeys, "error", err)
	}
}
