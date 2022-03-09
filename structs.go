package lsmdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/impact-eintr/lsmdb/y"
)

type valuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint32
}

// first: Fid secode: Offset thrid: Len
func (p valuePointer) Less(o valuePointer) bool {
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

func (p valuePointer) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

const vptrSize = 12 // 4 + 4 + 4

func (p valuePointer) Encode(b []byte) []byte {
	binary.BigEndian.PutUint32(b[:4], p.Fid)
	binary.BigEndian.PutUint32(b[4:8], p.Len)
	binary.BigEndian.PutUint32(b[8:12], p.Offset)
	return b[:vptrSize]
}

func (p *valuePointer) Decode(b []byte) {
	p.Fid = binary.BigEndian.Uint32(b[:4])
	p.Len = binary.BigEndian.Uint32(b[4:8])
	p.Offset = binary.BigEndian.Uint32(b[8:12])
}

// header is used int value log as a header before Entry.
type header struct {
	klen      uint32
	vlen      uint32
	expiresAt uint64
	meta      byte
	userMeta  byte
}

const (
	headerBufSize = 18 // 4 + 4 + 8 + 1 + 1
)

func (h header) Encode(out []byte) {
	y.AssertTrue(len(out) >= headerBufSize)
	binary.BigEndian.PutUint32(out[0:4], h.klen)
	binary.BigEndian.PutUint32(out[4:8], h.vlen)
	binary.BigEndian.PutUint64(out[8:16], h.expiresAt)
	out[16] = h.meta
	out[17] = h.userMeta
}

func (h *header) Decode(buf []byte) {
	h.klen = binary.BigEndian.Uint32(buf[0:4])
	h.vlen = binary.BigEndian.Uint32(buf[4:8])
	h.expiresAt = binary.BigEndian.Uint64(buf[8:16])
	h.meta = buf[16]
	h.userMeta = buf[17]
}

type Entry struct {
	Key       []byte
	Value     []byte
	UserMeta  byte
	ExpiresAt uint64 // time.Unix
	meta      byte

	// Fields maintained internally.
	offset uint32
}

// 如果存的值小于threshold 直接返回key+value+meta的大小 否则返回key+valuePointer+meta的大小
// estimate: 估计
func (e *Entry) estimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 2 // Meta, UserMeta
	}
	return len(e.Key) + 12 + 2 // 12 for ValuePointer, 2 for metas.
}

// 解码 Entry
func encodeEntry(e *Entry, buf *bytes.Buffer) (int, error) {
	// 初始化header
	h := header{
		klen:      uint32(len(e.Key)),
		vlen:      uint32(len(e.Value)),
		expiresAt: e.ExpiresAt,
		meta:      e.meta,
		userMeta:  e.UserMeta,
	}

	var headerEnc [headerBufSize]byte
	h.Encode(headerEnc[:])

	// 计算循环冗余码
	hash := crc32.New(y.CastagnoliCrcTable)

	buf.Write(headerEnc[:])
	hash.Write(headerEnc[:])

	buf.Write(e.Key) // 写key
	hash.Write(e.Key)

	buf.Write(e.Value)
	hash.Write(e.Value) // 写value

	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	buf.Write(crcBuf[:]) // 将循环冗余码追加到buf

	return len(headerEnc) + len(e.Key) + len(e.Value) + len(crcBuf), nil
}

func (e Entry) print(prefix string) {
	fmt.Printf("%s Key: %s Meta: %d UserMeta: %d Offset: %d len(val)=%d",
		prefix, e.Key, e.meta, e.UserMeta, e.offset, len(e.Value))
}
