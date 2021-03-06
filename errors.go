package lsmdb

import (
	"encoding/hex"

	"github.com/pkg/errors"
)

var (
	// ErrInvalidDir is returned when Badger cannot find the directory
	// from where it is supposed to load the key-value store.
	ErrInvalidDir = errors.New("Invalid Dir, directory does not exist")

	// ErrValueLogSize is returned when opt.ValueLogFileSize option is not within the valid
	// range.
	ErrValueLogSize = errors.New("Invalid ValueLogFileSize, must be between 1MB and 2GB")

	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")

	// ErrTxnTooBig is returned if too many writes are fit into a single transaction.
	ErrTxnTooBig = errors.New("Txn is too big to fit into one request")

	// ErrConflict is returned when a transaction conflicts with another transaction. This can happen if
	// the read rows had been updated concurrently by another transaction.
	ErrConflict = errors.New("Transaction Conflict. Please retry")

	// ErrReadOnlyTxn is returned if an update function is called on a read-only transaction.
	ErrReadOnlyTxn = errors.New("No sets or deletes are allowed in a read-only transaction")

	// ErrDiscardedTxn is returned if a previously discarded transaction is re-used.
	ErrDiscardedTxn = errors.New("This transaction has been discarded. Create a new one")

	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")

	// ErrRetry is returned when a log file containing the value is not found.
	// This usually indicates that it may have been garbage collected, and the
	// operation needs to be retried.
	ErrRetry = errors.New("Unable to find log file. Please retry")

	// ErrThresholdZero is returned if threshold is set to zero, and value log GC is called.
	// In such a case, GC can't be run.
	ErrThresholdZero = errors.New(
		"Value log GC can't run because threshold is set to zero")
	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New(
		"Value log GC attempt didn't result in any cleanup")

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected = errors.New("Value log GC request rejected") // reject: ??????

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")

	// ErrManagedTxn is returned if the user tries to use an API which isn't
	// allowed due to external management of transactions, when using ManagedDB.
	ErrManagedTxn = errors.New(
		"Invalid API request. Not allowed to perform this action using ManagedDB")

	// ErrInvalidDump if a data dump made previously cannot be loaded into the database.
	ErrInvalidDump = errors.New("Data dump cannot be read")
)

const maxKeySize = 1 << 20

func exceedsMaxKeySizeError(key []byte) error {
	return errors.Errorf("Key with size %d exceeded %dMiB limit. Key:\n%s",
		len(key), maxKeySize>>20, hex.Dump(key[:1<<10]))
}

func exceedsMaxValueSizeError(value []byte, maxValueSize int64) error {
	return errors.Errorf("Value with size %d exceeded ValueLogFileSize (%dMiB). Key:\n%s",
		len(value), maxValueSize>>20, hex.Dump(value[:1<<10]))
}
