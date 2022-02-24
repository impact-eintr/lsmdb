package y

import "expvar"

var (
	// LSMSize has size of the LSM in bytes
	LSMSize *expvar.Map
	// VlogSize has size of the value log in bytes
	VlogSize *expvar.Map
	// PendingWrites tracks the number of pending writes.
	PendingWrites *expvar.Map

	// These are cumulative

	// NumReads has cumulative number of reads
	NumReads *expvar.Int
	// NumWrites has cumulative number of writes
	NumWrites *expvar.Int
	// NumBytesRead has cumulative number of bytes read
	NumBytesRead *expvar.Int
	// NumBytesWritten has cumulative number of bytes written
	NumBytesWritten *expvar.Int
	// NumLSMGets is number of LMS gets
	NumLSMGets *expvar.Map
	// NumLSMBloomHits is number of LMS bloom hits
	NumLSMBloomHits *expvar.Map
	// NumGets is number of gets
	NumGets *expvar.Int
	// NumPuts is number of puts
	NumPuts *expvar.Int
	// NumBlockedPuts is number of blocked puts
	NumBlockedPuts *expvar.Int
	// NumMemtableGets is number of memtable gets
	NumMemtableGets *expvar.Int
)

// These variables are global and have cumulative values for all kv stores.
func init() {
	NumReads = expvar.NewInt("badger_disk_reads_total")
	NumWrites = expvar.NewInt("badger_disk_writes_total")
	NumBytesRead = expvar.NewInt("badger_read_bytes")
	NumBytesWritten = expvar.NewInt("badger_written_bytes")
	NumLSMGets = expvar.NewMap("badger_lsm_level_gets_total")
	NumLSMBloomHits = expvar.NewMap("badger_lsm_bloom_hits_total")
	NumGets = expvar.NewInt("badger_gets_total")
	NumPuts = expvar.NewInt("badger_puts_total")
	NumBlockedPuts = expvar.NewInt("badger_blocked_puts_total")
	NumMemtableGets = expvar.NewInt("badger_memtable_gets_total")
	LSMSize = expvar.NewMap("badger_lsm_size_bytes")
	VlogSize = expvar.NewMap("badger_vlog_size_bytes")
	PendingWrites = expvar.NewMap("badger_pending_writes_total")
}
