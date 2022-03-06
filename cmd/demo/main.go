package main

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v3"
)

func main() {

	// open the DB file
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 读写事务
	err = db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte("answer"), []byte("42"))
		txn.Set([]byte("answer_v1"), []byte("43"))
		txn.Set([]byte("answer_v2"), []byte("44"))
		txn.Set([]byte("answer_v3"), []byte("45"))
		txn.Get([]byte("answer"))
		txn.Delete([]byte("answer"))
		return nil
	})

	// 只读事务
	err = db.View(func(txn *badger.Txn) error {
		txn.Get([]byte("answer_v1"))
		return nil
	})

	// 遍历keys
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	// vlog 的GC
	err = db.RunValueLogGC(0.7)
	_ = err
}
