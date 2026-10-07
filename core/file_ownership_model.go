package core

import (
	"path"
	"strings"

	"github.com/pocketbase/pocketbase/tools/security"
	"github.com/pocketbase/pocketbase/tools/types"
)

var _ Model = (*FileOwnership)(nil)

const FileOwnershipTableName = "_fileOwnership"

// FileOwnership defines a persistent ownership (attribution) record for a
// single PocketBase-managed storage file.
//
// The records are stored in the auxiliary database and act as the source of
// truth when reconciling the storage with the db record file references:
//   - a storage file with an ownership record is considered "managed";
//   - a storage file without an ownership record is considered "external"
//     (eg. manually placed by an external tool) and is never touched
//     automatically.
//
// Each record keeps a digest of the file content (allowing to detect
// externally modified files) and a list of all currently known derived
// thumbnail keys (allowing them to be cleaned up together with the original).
type FileOwnership struct {
	BaseModel

	// Collection is the id of the collection owning the file.
	Collection string `db:"collection" json:"collection"`

	// Record is the id of the record owning the file.
	Record string `db:"record" json:"record"`

	// Field is the name of the file field the file is assigned to.
	Field string `db:"field" json:"field"`

	// FileKey is the full storage key of the managed file
	// (eg. "collectionId/recordId/filename_random.txt").
	FileKey string `db:"fileKey" json:"fileKey"`

	// Name is the plain file name (the value stored in the record field).
	Name string `db:"name" json:"name"`

	// Digest is the hex encoded sha256 digest of the file content.
	Digest string `db:"digest" json:"digest"`

	// Size is the last known file size in bytes.
	Size int64 `db:"size" json:"size"`

	// Thumbs holds the storage keys of all currently known derived thumbnails.
	Thumbs types.JSONArray[string] `db:"thumbs" json:"thumbs"`

	Created types.DateTime `db:"created" json:"created"`
	Updated types.DateTime `db:"updated" json:"updated"`
}

// TableName implements [Model.TableName].
func (m *FileOwnership) TableName() string {
	return FileOwnershipTableName
}

// ThumbsPrefix returns the storage prefix under which all derived thumbnails
// of the managed file are stored (eg. "coll/rec/thumbs_name_xxx.jpg/").
func (m *FileOwnership) ThumbsPrefix() string {
	return thumbsPrefixForFileKey(m.FileKey)
}

// thumbsPrefixForFileKey returns the storage thumbs directory prefix
// for the provided original file key.
//
// Expects the file key to be in the canonical "<collection>/<record>/<name>" form.
func thumbsPrefixForFileKey(fileKey string) string {
	dir, name := path.Split(fileKey)
	return dir + "thumbs_" + name + "/"
}

// originalFileKeyFromThumbsKey returns the original file key for the provided
// thumb storage key and whether the key is actually located under a
// "thumbs_<name>" directory.
//
// Example:
//
//	originalFileKeyFromThumbsKey("coll/rec/thumbs_a.jpg/100x100_a.jpg")
//	  // => "coll/rec/a.jpg", true
func originalFileKeyFromThumbsKey(thumbKey string) (string, bool) {
	const marker = "/thumbs_"

	idx := strings.LastIndex(thumbKey, marker)
	if idx <= 0 {
		return "", false
	}

	rest := thumbKey[idx+len(marker):]

	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return "", false
	}

	originalName := rest[:slash]
	if originalName == "" {
		return "", false
	}

	return thumbKey[:idx] + "/" + originalName, true
}

// generateFileOwnershipId returns a deterministic id for the provided file key
// so that ownership upserts are idempotent and crash safe.
func generateFileOwnershipId(fileKey string) string {
	return security.SHA256(fileKey)
}
