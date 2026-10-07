package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"runtime"
	"sort"
)

type Migration struct {
	Up               func(txApp App) error
	Down             func(txApp App) error
	File             string
	Identity         string
	ReapplyCondition func(txApp App, runner *MigrationsRunner, fileName string) (bool, error)
}

// MigrationIdentity computes a stable content-based identity of a migration source.
//
// It can be used to associate an applied migration history entry with its
// current file even when the file was renamed across branches/deployments.
//
// Line endings are normalized to LF so that the identity stays the same
// across checkouts on different operating systems.
func MigrationIdentity(content []byte) string {
	normalized := bytes.ReplaceAll(content, []byte("\r\n"), []byte("\n"))
	normalized = bytes.ReplaceAll(normalized, []byte("\r"), []byte("\n"))

	sum := sha256.Sum256(normalized)

	return hex.EncodeToString(sum[:])
}

// MigrationsList defines a list with migration definitions
type MigrationsList struct {
	list []*Migration
}

// Item returns a single migration from the list by its index.
func (l *MigrationsList) Item(index int) *Migration {
	return l.list[index]
}

// Items returns the internal migrations list slice.
func (l *MigrationsList) Items() []*Migration {
	return l.list
}

// Copy copies all provided list migrations into the current one.
func (l *MigrationsList) Copy(list MigrationsList) {
	for _, item := range list.Items() {
		l.Add(&Migration{
			Up:               item.Up,
			Down:             item.Down,
			File:             item.File,
			Identity:         item.Identity,
			ReapplyCondition: item.ReapplyCondition,
		})
	}
}

// Add adds adds an existing migration definition to the list.
//
// If m.File is not provided, it will try to get the name from its .go file.
//
// The list will be sorted automatically based on the migrations file name.
func (l *MigrationsList) Add(m *Migration) {
	if m.File == "" {
		_, path, _, _ := runtime.Caller(1)
		m.File = filepath.Base(path)
	}

	l.list = append(l.list, m)

	sort.SliceStable(l.list, func(i int, j int) bool {
		return l.list[i].File < l.list[j].File
	})
}

// Register adds new migration definition to the list.
//
// If optFilename is not provided, it will try to get the name from its .go file.
//
// The list will be sorted automatically based on the migrations file name.
func (l *MigrationsList) Register(
	up func(txApp App) error,
	down func(txApp App) error,
	optFilename ...string,
) {
	var file string
	if len(optFilename) > 0 {
		file = optFilename[0]
	} else {
		_, path, _, _ := runtime.Caller(1)
		file = filepath.Base(path)
	}

	l.list = append(l.list, &Migration{
		File: file,
		Up:   up,
		Down: down,
	})

	sort.SliceStable(l.list, func(i int, j int) bool {
		return l.list[i].File < l.list[j].File
	})
}
