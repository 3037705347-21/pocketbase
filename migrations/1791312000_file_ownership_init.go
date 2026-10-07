package migrations

import (
	"github.com/pocketbase/pocketbase/core"
)

func init() {
	core.SystemMigrations.Add(&core.Migration{
		Up: func(txApp core.App) error {
			_, execErr := txApp.AuxDB().NewQuery(`
				CREATE TABLE IF NOT EXISTS {{_fileOwnership}} (
					[[id]]         TEXT PRIMARY KEY NOT NULL,
					[[collection]] TEXT DEFAULT '' NOT NULL,
					[[record]]     TEXT DEFAULT '' NOT NULL,
					[[field]]      TEXT DEFAULT '' NOT NULL,
					[[fileKey]]    TEXT NOT NULL UNIQUE,
					[[name]]       TEXT DEFAULT '' NOT NULL,
					[[digest]]     TEXT DEFAULT '' NOT NULL,
					[[size]]       INTEGER DEFAULT 0 NOT NULL,
					[[thumbs]]     JSON DEFAULT '[]' NOT NULL,
					[[created]]    TEXT DEFAULT '' NOT NULL,
					[[updated]]    TEXT DEFAULT '' NOT NULL
				);

				CREATE UNIQUE INDEX IF NOT EXISTS idx_file_ownership_fileKey on {{_fileOwnership}} ([[fileKey]]);
				CREATE INDEX IF NOT EXISTS idx_file_ownership_owner on {{_fileOwnership}} ([[collection]], [[record]]);
			`).Execute()

			return execErr
		},
		Down: func(txApp core.App) error {
			_, err := txApp.AuxDB().DropTable("_fileOwnership").Execute()
			return err
		},
		ReapplyCondition: func(txApp core.App, runner *core.MigrationsRunner, fileName string) (bool, error) {
			// reapply only if the _fileOwnership table doesn't exist
			exists := txApp.AuxHasTable("_fileOwnership")
			return !exists, nil
		},
	})
}
