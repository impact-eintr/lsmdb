package lsmdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"sync"

	"github.com/impact-eintr/lsmdb/protos"
	"github.com/impact-eintr/lsmdb/y"
)

func writeTo(Entry *protos.KVPair, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(Entry.Size())); err != nil {
		return err
	}
	buf, err := Entry.Marshal()
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (db *DB) Backup(w io.Writer, since uint64) (uint64, error) {
	var tsNew uint64
	err := db.View(func(txn *Txn) error {
		opts := DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item.Version() < since {
				// 跳过过时数据版本
				continue
			}
			val, err := item.Value()
			if err != nil {
				return err
			}

			Entry := &protos.KVPair{
				Key:       y.Copy(item.Key()),
				Value:     y.Copy(val),
				UserMeta:  []byte{item.UserMeta()},
				Version:   item.Version(),
				ExpiresAt: item.ExpiresAt(),
			}

			// Write entries to disk
			if err := writeTo(Entry, w); err != nil {
				return err
			}
		}
		tsNew = txn.readTs
		return nil
	})
	return tsNew, err
}

func (db *DB) Load(r io.Reader) error {
	br := bufio.NewReaderSize(r, 16<<10)
	unmarshalBuf := make([]byte, 1<<10)
	var entries []*Entry
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	// NOTE 这个写法太炫了
	batchSetAsyncIfNoErr := func(entries []*Entry) error {
		select {
		case err := <-errChan:
			return err
		default:
			wg.Add(1)
			return db.batchSetAsync(entries, func(err error) {
				defer wg.Done()
				if err != nil {
					select {
					case errChan <- err:
					default:
					}
				}
			})
		}
	}

	for {
		var sz uint64
		err := binary.Read(br, binary.LittleEndian, &sz)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if cap(unmarshalBuf) < int(sz) {
			unmarshalBuf = make([]byte, sz)
		}

		e := &protos.KVPair{}
		if _, err := io.ReadFull(br, unmarshalBuf[:sz]); err != nil {
			return err
		}
		if err = e.Unmarshal(unmarshalBuf[:sz]); err != nil {
			return err
		}
		entries = append(entries, &Entry{
			Key:       y.KeyWithTs(e.Key, e.Version),
			Value:     e.Value,
			UserMeta:  e.UserMeta[0],
			ExpiresAt: e.ExpiresAt,
		})

		if len(entries) == 1000 {
			if err := batchSetAsyncIfNoErr(entries); err != nil {
				return err
			}
			entries = entries[:0]
		}
	}

	if len(entries) > 0 {
		if err := batchSetAsyncIfNoErr(entries); err != nil {
			return err
		}
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}
