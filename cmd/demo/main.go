package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/impact-eintr/lsmdb"
)

func main() {

	// open the DB file
	db, err := lsmdb.Open(lsmdb.DefaultOptions("/tmp/lsmdb"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 读写事务
	err = db.Update(func(txn *lsmdb.Txn) error {
		kvb := []byte{}
		for i := 0; i < 5; i++ {
			kvb = appendKV(kvb, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)), 1)
			buf := make([]byte, len(kvb))
			copy(buf, kvb)
			txn.Set([]byte(fmt.Sprintf("test%d", i)), buf)
		}
		return nil
	})

	// 遍历keys
	err = db.View(func(txn *lsmdb.Txn) error {
		opts := lsmdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			v, err := item.Value()
			counter := 0
			rangeKV(v, func(k, v []byte) error {
				if !bytes.Equal(k, []byte(fmt.Sprintf("key%d", counter))) ||
					!bytes.Equal(v, []byte(fmt.Sprintf("value%d", counter))) {
					log.Println(string(k), string(v))
				}
				counter++
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	// 删除这些键
	err = db.Update(func(txn *lsmdb.Txn) error {
		for i := 0; i < 200; i++ {
			txn.Delete([]byte(fmt.Sprintf("test%d", i)))
		}
		return nil
	})

	// vlog 的GC
	err = db.RunValueLogGC(0.7)
	_ = err
}
