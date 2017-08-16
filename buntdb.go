// Package buntdb implements a low-level in-memory key/value store in pure Go.
// It persists to disk, is ACID compliant, and uses locking for multiple
// readers and a single writer. Bunt is ideal for projects that need
// a dependable database, and favor speed over data size.
package buntdb

import (
	"errors"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/gjson"
	"github.com/tidwall/match"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")
)

// DB represents a collection of key-value pairs that persist on disk.
// Transactions are used for all forms of data access to the DB.
type DB struct {
	mu     sync.RWMutex      // the gatekeeper for all fields
	keys   *btree.BTree      // a tree of all item ordered by key
	exps   *btree.BTree      // a tree of items ordered by expiration
	idxs   map[string]*index // the index trees.
	exmgr  bool              // indicates that expires manager is running.
	closed bool              // set when the database has been closed
	config Config            // the database configuration
}

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config struct {
	// OnExpired is used to custom handle the deletion option when a key
	// has been expired.
	OnExpired func(keys []string)
}

// exctx is a simple b-tree context for ordering by expiration.
type exctx struct {
	db *DB
}

// Default number of btree degrees
const btreeDegrees = 64

// Open opens a database at the provided path.
// If the file does not exist then it will be created automatically.
func Open(path string) (*DB, error) {
	db := &DB{}
	// initialize trees and indexes
	db.keys = btree.New(btreeDegrees, nil)
	db.exps = btree.New(btreeDegrees, &exctx{db})
	db.idxs = make(map[string]*index)
	// initialize default configuration
	db.config = Config{}
	// turn off persistence for pure in-memory
	// start the background manager.
	go db.backgroundManager()
	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true
	// Let's release all references to nil. This will help both with debugging
	// late usage panics and it provides a hint to the garbage collector
	db.keys, db.exps, db.idxs = nil, nil, nil
	return nil
}

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
type index struct {
	btr     *btree.BTree               // contains the items
	name    string                     // name of the index
	pattern string                     // a required key pattern
	less    func(a, b btree.Item) bool // less comparison function
	db      *DB                        // the origin database
	opts    IndexOptions               // index options
}

// match matches the pattern to the key
func (idx *index) match(key string) bool {
	if idx.pattern == "*" {
		return true
	}
	if idx.opts.CaseInsensitiveKeyMatching {
		for i := 0; i < len(key); i++ {
			if key[i] >= 'A' && key[i] <= 'Z' {
				key = strings.ToLower(key)
				break
			}
		}
	}
	return match.Match(key, idx.pattern)
}

// clearCopy creates a copy of the index, but with an empty dataset.
func (idx *index) clearCopy() *index {
	// copy the index meta information
	nidx := &index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		opts:    idx.opts,
	}
	// initialize with empty trees
	if nidx.less != nil {
		nidx.btr = btree.New(btreeDegrees, nidx)
	}
	return nidx
}

// rebuild rebuilds the index
func (idx *index) rebuild() {
	// initialize trees
	if idx.less != nil {
		idx.btr = btree.New(btreeDegrees, idx)
	}
	// iterate through all keys and fill the index
	idx.db.keys.Ascend(func(item btree.Item) bool {
		dbi := item.(*dbItem)
		if !idx.match(dbi.key) {
			// does not match the pattern, conintue
			return true
		}
		if idx.less != nil {
			idx.btr.ReplaceOrInsert(dbi)
		}
		return true
	})
}

// Indexes returns a list of index names.
//
// Deprecated: Use Transactions
func (db *DB) Indexes() ([]string, error) {
	var names []string
	var err = db.View(func(tx *Tx) error {
		var err error
		names, err = tx.Indexes()
		return err
	})
	return names, err
}

// ReadConfig returns the database configuration.
func (db *DB) ReadConfig(config *Config) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig updates the database configuration.
func (db *DB) SetConfig(config Config) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.config = config
	return nil
}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB) insertIntoDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.ReplaceOrInsert(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*dbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
		}
	}
	if item.opts != nil && item.opts.ex {
		// The new item has eviction options. Add it to the
		// expires tree
		db.exps.ReplaceOrInsert(item)
	}
	for _, idx := range db.idxs {
		if !idx.match(item.key) {
			continue
		}
		if idx.btr != nil {
			// Add new item to btree index.
			idx.btr.ReplaceOrInsert(item)
		}
	}
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *DB) deleteFromDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*dbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
		}
	}
	return pdbi
}

// backgroundManager runs continuously in the background and performs various
// operations such as removing expired items and syncing to disk.
func (db *DB) backgroundManager() {
	flushes := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		// Open a standard view. This will take a full lock of the
		// database thus allowing for access to anything we need.
		var onExpired func([]string)
		var expired []string
		err := db.Update(func(tx *Tx) error {
			onExpired = db.config.OnExpired
			// produce a list of expired items that need removing
			db.exps.AscendLessThan(&dbItem{
				opts: &dbItemOpts{ex: true, exat: time.Now()},
			}, func(item btree.Item) bool {
				expired = append(expired, item.(*dbItem).key)
				return true
			})
			if onExpired == nil {
				for _, key := range expired {
					if _, err := tx.Delete(key); err != nil {
						// it's ok to get a "not found" because the
						// 'Delete' method reports "not found" for
						// expired items.
						if err != ErrNotFound {
							return err
						}
					}
				}
			}
			return nil
		})
		if err == ErrDatabaseClosed {
			break
		}

		// send expired event, if needed
		if onExpired != nil && len(expired) > 0 {
			onExpired(expired)
		}
	}
}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must rollback.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	return
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// get return an item or nil if not found.
func (db *DB) get(key string) *dbItem {
	item := db.keys.Get(&dbItem{key: key})
	if item != nil {
		return item.(*dbItem)
	}
	return nil
}

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
type Tx struct {
	db       *DB             // the underlying database.
	writable bool            // when false mutable operations fail.
	funcd    bool            // when true Commit and Rollback panic.
	wc       *txWriteContext // context for writable transactions.
}

type txWriteContext struct {
	// rollback when deleteAll is called
	rbkeys *btree.BTree      // a tree of all item ordered by key
	rbexps *btree.BTree      // a tree of items ordered by expiration
	rbidxs map[string]*index // the index trees.

	rollbackItems   map[string]*dbItem // details for rolling back tx.
	commitItems     map[string]*dbItem // details for committing tx.
	itercount       int                // stack of iterators
	rollbackIndexes map[string]*index  // details for dropped indexes.
}

// DeleteAll deletes all items from the database.
func (tx *Tx) DeleteAll() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}

	// check to see if we've already deleted everything
	if tx.wc.rbkeys == nil {
		// we need to backup the live data in case of a rollback.
		tx.wc.rbkeys = tx.db.keys
		tx.wc.rbexps = tx.db.exps
		tx.wc.rbidxs = tx.db.idxs
	}

	// now reset the live database trees
	tx.db.keys = btree.New(btreeDegrees, nil)
	tx.db.exps = btree.New(btreeDegrees, &exctx{tx.db})
	tx.db.idxs = make(map[string]*index)

	// finally re-create the indexes
	for name, idx := range tx.wc.rbidxs {
		tx.db.idxs[name] = idx.clearCopy()
	}

	// always clear out the commits
	tx.wc.commitItems = make(map[string]*dbItem)

	return nil
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
//
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		// writable transactions have a writeContext object that
		// contains information about changes to the database.
		tx.wc = &txWriteContext{}
		tx.wc.rollbackItems = make(map[string]*dbItem)
		tx.wc.rollbackIndexes = make(map[string]*index)
	}
	return tx, nil
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// rollbackInner handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx) rollbackInner() {
	// rollback the deleteAll if needed
	if tx.wc.rbkeys != nil {
		tx.db.keys = tx.wc.rbkeys
		tx.db.idxs = tx.wc.rbidxs
		tx.db.exps = tx.wc.rbexps
	}
	for key, item := range tx.wc.rollbackItems {
		tx.db.deleteFromDatabase(&dbItem{key: key})
		if item != nil {
			// When an item is not nil, we will need to reinsert that item
			// into the database overwriting the current one.
			tx.db.insertIntoDatabase(item)
		}
	}
	for name, idx := range tx.wc.rollbackIndexes {
		delete(tx.db.idxs, name)
		if idx != nil {
			// When an index is not nil, we will need to rebuilt that index
			// this could be an expensive process if the database has many
			// items or the index is complex.
			tx.db.idxs[name] = idx
			idx.rebuild()
		}
	}
}

// Commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
func (tx *Tx) Commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	var err error
	// Unlock the database and allow for another writable transaction.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return err
}

// Rollback closes the transaction and reverts all mutable operations that
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
func (tx *Tx) Rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	if tx.writable {
		tx.rollbackInner()
	}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// dbItemOpts holds various meta information about an item.
type dbItemOpts struct {
	ex   bool      // does this item expire?
	exat time.Time // when does this item expire?
}
type dbItem struct {
	key  string // the binary key
	val  btree.Item
	opts *dbItemOpts // optional meta information
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(count), 10)...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = append(buf, strconv.FormatInt(int64(len(s)), 10)...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
func (dbi *dbItem) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.key)
	return buf
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *dbItem) expired() bool {
	return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// MaxTime from http://stackoverflow.com/questions/25065055#32620397
// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// expiresAt will return the time when the item will expire. When an item does
// not expire `maxTime` is used.
func (dbi *dbItem) expiresAt() time.Time {
	if dbi.opts == nil || !dbi.opts.ex {
		return maxTime
	}
	return dbi.opts.exat
}

// Less determines if a b-tree item is less than another. This is required
// for ordering, inserting, and deleting items from a b-tree. It's important
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *dbItem) Less(item btree.Item, ctx interface{}) bool {
	dbi2 := item.(*dbItem)
	switch ctx := ctx.(type) {
	case *exctx:
		// The expires b-tree formula
		if dbi2.expiresAt().After(dbi.expiresAt()) {
			return true
		}
		if dbi.expiresAt().After(dbi2.expiresAt()) {
			return false
		}
	case *index:
		if ctx.less != nil {
			// Using an index
			if ctx.less(dbi.val, dbi2.val) {
				return true
			}
			if ctx.less(dbi2.val, dbi.val) {
				return false
			}
		}
	}
	// Always fall back to the key comparison. This creates absolute uniqueness.
	return dbi.key < dbi2.key
}

// SetOptions represents options that may be included with the Set() command.
type SetOptions struct {
	// Expires indicates that the Set() key-value will expire
	Expires bool
	// TTL is how much time the key-value will exist in the database
	// before being evicted. The Expires field must also be set to true.
	// TTL stands for Time-To-Live.
	TTL time.Duration
}

// GetLess returns the less function for an index. This is handy for
// doing ad-hoc compares inside a transaction.
// Returns ErrNotFound if the index is not found or there is no less
// function bound to the index
func (tx *Tx) GetLess(index string) (func(a, b btree.Item) bool, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.idxs[index]
	if !ok || idx.less == nil {
		return nil, ErrNotFound
	}
	return idx.less, nil
}

// Set inserts or replaces an item in the database based on the key.
// The opt params may be used for additional functionality such as forcing
// the item to be evicted at a specified time. When the return value
// for err is nil the operation succeeded. When the return value of
// replaced is true, then the operaton replaced an existing item whose
// value will be returned through the previousValue variable.
// The results of this operation will not be available to other
// transactions until the current transaction has successfully committed.
//
// Only a writable transaction can be used with this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Set(key string, value btree.Item, opts *SetOptions) (previousValue btree.Item,
	replaced bool, err error) {
	if tx.db == nil {
		return nil, false, ErrTxClosed
	} else if !tx.writable {
		return nil, false, ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return nil, false, ErrTxIterating
	}
	item := &dbItem{key: key, val: value}
	if opts != nil {
		if opts.Expires {
			// The caller is requesting that this item expires. Convert the
			// TTL to an absolute time and bind it to the item.
			item.opts = &dbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
		}
	}
	// Insert the item into the keys tree.
	prev := tx.db.insertIntoDatabase(item)

	// insert into the rollback map if there has not been a deleteAll.
	if tx.wc.rbkeys == nil {
		if prev == nil {
			// An item with the same key did not previously exist. Let's
			// create a rollback entry with a nil value. A nil value indicates
			// that the entry should be deleted on rollback. When the value is
			// *not* nil, that means the entry should be reverted.
			tx.wc.rollbackItems[key] = nil
		} else {
			// A previous item already exists in the database. Let's create a
			// rollback entry with the item as the value. We need to check the
			// map to see if there isn't already an item that matches the
			// same key.
			if _, ok := tx.wc.rollbackItems[key]; !ok {
				tx.wc.rollbackItems[key] = prev
			}
			if !prev.expired() {
				previousValue, replaced = prev.val, true
			}
		}
	}
	return previousValue, replaced, nil
}

// Get returns a value for a key. If the item does not exist or if the item
// has expired then ErrNotFound is returned.
func (tx *Tx) Get(key string) (val btree.Item, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	item := tx.db.get(key)
	if item == nil || item.expired() {
		// The item does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return nil, ErrNotFound
	}
	return item.val, nil
}

// Delete removes an item from the database based on the item's key. If the item
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Delete(key string) (val btree.Item, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if !tx.writable {
		return nil, ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return nil, ErrTxIterating
	}
	item := tx.db.deleteFromDatabase(&dbItem{key: key})
	if item == nil {
		return nil, ErrNotFound
	}
	// create a rollback entry if there has not been a deleteAll call.
	if tx.wc.rbkeys == nil {
		if _, ok := tx.wc.rollbackItems[key]; !ok {
			tx.wc.rollbackItems[key] = item
		}
	}
	// Even though the item has been deleted, we still want to check
	// if it has expired. An expired item should not be returned.
	if item.expired() {
		// The item exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return nil, ErrNotFound
	}
	return item.val, nil
}

// TTL returns the remaining time-to-live for an item.
// A negative duration will be returned for items that do not have an
// expiration.
func (tx *Tx) TTL(key string) (time.Duration, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	item := tx.db.get(key)
	if item == nil {
		return 0, ErrNotFound
	} else if item.opts == nil || !item.opts.ex {
		return -1, nil
	}
	dur := item.opts.exat.Sub(time.Now())
	if dur < 0 {
		return 0, ErrNotFound
	}
	return dur, nil
}

// scan iterates through a specified index and calls user-defined iterator
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// The gt param indicates that there is a greaterThan limit.
// The lt param indicates that there is a lessThan limit.
// The index param tells the scanner to use the specified index tree. An
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx) scan(desc, gt, lt bool, index, start, stop string,
	iterator func(key string, value btree.Item) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi := item.(*dbItem)
		return iterator(dbi.key, dbi.val)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means we will use the keys tree.
		tr = tx.db.keys
	} else {
		idx := tx.db.idxs[index]
		if idx == nil {
			// index was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *dbItem
	if gt || lt {
		if index == "" {
			itemA = &dbItem{key: start}
			itemB = &dbItem{key: stop}
		} else {
			log.Panicln("Error, index given", index, start, stop)
			// itemA = &dbItem{val: start}
			// itemB = &dbItem{val: stop}
		}
	}
	// execute the scan on the underlying tree.
	if tx.wc != nil {
		tx.wc.itercount++
		defer func() {
			tx.wc.itercount--
		}()
	}
	if desc {
		if gt {
			if lt {
				tr.DescendRange(itemA, itemB, iter)
			} else {
				tr.DescendGreaterThan(itemA, iter)
			}
		} else if lt {
			tr.DescendLessOrEqual(itemA, iter)
		} else {
			tr.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				tr.AscendRange(itemA, itemB, iter)
			} else {
				tr.AscendGreaterOrEqual(itemA, iter)
			}
		} else if lt {
			tr.AscendLessThan(itemA, iter)
		} else {
			tr.Ascend(iter)
		}
	}
	return nil
}

// Match returns true if the specified key matches the pattern. This is a very
// simple pattern matcher where '*' matches on any number characters and '?'
// matches on any one character.
func Match(key, pattern string) bool {
	return match.Match(key, pattern)
}

// AscendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) AscendKeys(pattern string,
	iterator func(key string, value btree.Item) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Ascend("", iterator)
		}
		return tx.Ascend("", func(key string, value btree.Item) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.AscendGreaterOrEqual("", min, func(key string, value btree.Item) bool {
		if key > max {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// DescendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) DescendKeys(pattern string,
	iterator func(key string, value btree.Item) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Descend("", iterator)
		}
		return tx.Descend("", func(key string, value btree.Item) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.DescendLessOrEqual("", max, func(key string, value btree.Item) bool {
		if key < min {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Ascend(index string,
	iterator func(key string, value btree.Item) bool) error {
	return tx.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendGreaterOrEqual(index, pivot string,
	iterator func(key string, value btree.Item) bool) error {
	return tx.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendLessThan(index, pivot string,
	iterator func(key string, value btree.Item) bool) error {
	return tx.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key string, value btree.Item) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Descend(index string,
	iterator func(key string, value btree.Item) bool) error {
	return tx.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendGreaterThan(index, pivot string,
	iterator func(key string, value btree.Item) bool) error {
	return tx.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendLessOrEqual(index, pivot string,
	iterator func(key string, value btree.Item) bool) error {
	return tx.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key string, value btree.Item) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// Len returns the number of items in the database
func (tx *Tx) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	return tx.db.keys.Len(), nil
}

// IndexOptions provides an index with additional features or
// alternate functionality.
type IndexOptions struct {
	// CaseInsensitiveKeyMatching allow for case-insensitive
	// matching on keys when setting key/values.
	CaseInsensitiveKeyMatching bool
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (tx *Tx) CreateIndex(name, pattern string,
	less ...func(a, b btree.Item) bool) error {
	return tx.createIndex(name, pattern, less, nil)
}

// CreateIndexOptions is the same as CreateIndex except that it allows
// for additional options.
func (tx *Tx) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b btree.Item) bool) error {
	return tx.createIndex(name, pattern, less, opts)
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
func (tx *Tx) createIndex(name string, pattern string,
	lessers []func(a, b btree.Item) bool,
	opts *IndexOptions,
) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot create an index without a name.
		// an empty name index is designated for the main "keys" tree.
		return ErrIndexExists
	}
	// check if an index with that name already exists.
	if _, ok := tx.db.idxs[name]; ok {
		// index with name already exists. error.
		return ErrIndexExists
	}
	// genreate a less function
	var less func(a, b btree.Item) bool
	switch len(lessers) {
	default:
		// multiple less functions specified.
		// create a compound less function.
		less = func(a, b btree.Item) bool {
			for i := 0; i < len(lessers)-1; i++ {
				if lessers[i](a, b) {
					return true
				}
				if lessers[i](b, a) {
					return false
				}
			}
			return lessers[len(lessers)-1](a, b)
		}
	case 0:
		// no less function
	case 1:
		less = lessers[0]
	}
	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// intialize new index
	idx := &index{
		name:    name,
		pattern: pattern,
		less:    less,
		db:      tx.db,
		opts:    sopts,
	}
	idx.rebuild()
	// save the index
	tx.db.idxs[name] = idx
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use nil to indicate that the index should be removed upon rollback.
			tx.wc.rollbackIndexes[name] = nil
		}
	}
	return nil
}

// DropIndex removes an index.
func (tx *Tx) DropIndex(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot drop the default "keys" index
		return ErrInvalidOperation
	}
	idx, ok := tx.db.idxs[name]
	if !ok {
		return ErrNotFound
	}
	// delete from the map.
	// this is all that is needed to delete an index.
	delete(tx.db.idxs, name)
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use a non-nil copy of the index without the data to indicate that the
			// index should be rebuilt upon rollback.
			tx.wc.rollbackIndexes[name] = idx.clearCopy()
		}
	}
	return nil
}

// Indexes returns a list of index names.
func (tx *Tx) Indexes() ([]string, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	names := make([]string, 0, len(tx.db.idxs))
	for name := range tx.db.idxs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
func IndexString(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// This compares the raw binary of the string.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// This compares uint64s that are added to the database using the
// Uint() conversion function.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a helper function that returns true if 'a' is less than 'b'.
// This compares float64s that are added to the database using the
// Float() conversion function.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON provides for the ability to create an index on any JSON field.
// When the field is a string, the comparison will be case-insensitive.
// It returns a helper function used by CreateIndex.
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), false)
	}
}

// IndexJSONCaseSensitive provides for the ability to create an index on
// any JSON field.
// When the field is a string, the comparison will be case-sensitive.
// It returns a helper function used by CreateIndex.
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), true)
	}
}

// Desc is a helper function that changes the order of an index.
func Desc(less func(a, b string) bool) func(a, b string) bool {
	return func(a, b string) bool { return less(b, a) }
}
