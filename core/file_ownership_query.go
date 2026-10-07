package core

import (
	"github.com/pocketbase/dbx"
)

// FindFileOwnershipByKey returns the [FileOwnership] record associated
// with the provided storage file key.
//
// If no ownership record exists, it returns sql.ErrNoRows.
func (app *BaseApp) FindFileOwnershipByKey(fileKey string) (*FileOwnership, error) {
	m := &FileOwnership{}

	err := app.AuxModelQuery(m).
		AndWhere(dbx.HashExp{"fileKey": fileKey}).
		Limit(1).
		One(m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// FindAllFileOwnerships returns all ownership records matching the optional
// db expressions, ordered by their file key for deterministic results.
func (app *BaseApp) FindAllFileOwnerships(exprs ...dbx.Expression) ([]*FileOwnership, error) {
	result := []*FileOwnership{}

	query := app.AuxModelQuery(&FileOwnership{})
	for _, expr := range exprs {
		query = query.AndWhere(expr)
	}

	err := query.OrderBy("fileKey").All(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// upsertFileOwnership inserts or updates the provided ownership record
// (it is a no-op for a nil model).
func (app *BaseApp) upsertFileOwnership(m *FileOwnership) error {
	if m == nil {
		return nil
	}

	return app.AuxSaveNoValidate(m)
}

// deleteFileOwnershipByKeys deletes the ownership records associated with
// the provided storage file keys (nonexistent records are ignored).
func (app *BaseApp) deleteFileOwnershipByKeys(fileKeys ...string) error {
	cleaned := make([]any, 0, len(fileKeys))
	for _, key := range fileKeys {
		if key != "" {
			cleaned = append(cleaned, key)
		}
	}
	if len(cleaned) == 0 {
		return nil
	}

	_, err := app.AuxNonconcurrentDB().
		Delete(FileOwnershipTableName, dbx.In("fileKey", cleaned...)).
		Execute()
	if err != nil {
		return err
	}

	return nil
}
