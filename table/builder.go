package table

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/AndreasBriese/bbloom"
	"github.com/impact-eintr/lsmdb/y"
)

var (
	resultInterval = 100 // <ight want to change this to be based on total size instead of numKeys
)

func newBuffer(sz int) *bytes.Buffer {
	b := new(bytes.Buffer)
	b.Grow(sz)
	return b
}

type header struct {
	plen uint16 // Overlap(重叠) with base key
	klen uint16 // Lenght of the diff
	vlen uint16 // Lenght of value
	prev uint32 // Offset fot the prveious key-value pair.The offset os relative(相对的) to block bas offset
}

func (h header) Encode(b []byte) {
	binary.BigEndian.PutUint16(b[0:2], h.plen)
	binary.BigEndian.PutUint16(b[2:4], h.klen)
	binary.BigEndian.PutUint16(b[4:6], h.vlen)
	binary.BigEndian.PutUint32(b[6:10], h.prev)
}

func (h *header) Decode(buf []byte) int {
	h.plen = binary.BigEndian.Uint16(buf[0:2])
	h.klen = binary.BigEndian.Uint16(buf[2:4])
	h.vlen = binary.BigEndian.Uint16(buf[4:6])
	h.prev = binary.BigEndian.Uint32(buf[6:10])
	return h.Size()
}

func (h *header) Size() int { return 10 }

type Builder struct {
	counter    int
	buf        *bytes.Buffer
	baseKey    []byte
	baseOffset uint32

	restarts []uint32

	prevOffset uint32

	keyBuf   *bytes.Buffer
	keyCount int
}

func NewTableBuilder() *Builder {
	return &Builder{
		keyBuf:     newBuffer(1 << 20),
		buf:        newBuffer(1 << 20),
		prevOffset: math.MaxUint32, // Used for the first element
	}
}

func (b *Builder) Close() {}

func (b *Builder) Empty() bool { return b.buf.Len() == 0 }

// keyDiff returns a suffix of newKey that is different from b.baseKey.
func (b Builder) keyDiff(newKey []byte) []byte {
	var i int
	for i := 0; i < len(newKey) && i < len(b.baseKey); i++ {
		if newKey[i] != b.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (b *Builder) addHelper(key []byte, v y.ValueStruct) {
	// Add key to bloom filter
	if len(key) > 0 {
		var klen [2]byte
		keyNoTs := y.ParseKey(key)
		binary.BigEndian.PutUint16(klen[:], uint16(len(keyNoTs)))
		b.keyBuf.Write(klen[:])
		b.keyBuf.Write(keyNoTs)
		b.keyCount++
	}

	var diffKey []byte
	if len(b.baseKey) == 0 {
		b.baseKey = append(b.baseKey, key...)
		diffKey = key
	} else {
		diffKey = b.keyDiff(key)
	}

	h := header{
		plen: uint16(len(key) - len(diffKey)),
		klen: uint16(len(diffKey)),
		vlen: uint16(v.EncodedSize()),
		prev: b.prevOffset,
	}
	b.prevOffset = uint32(b.buf.Len()) - b.baseOffset

	var hbuf [10]byte
	h.Encode(hbuf[:])
	b.buf.Write(hbuf[:])
	b.buf.Write(diffKey)

	v.EncodeTo(b.buf)
	b.counter++
}

// 通过添加空结构表示结束
func (b *Builder) finishBlock() {
	b.addHelper([]byte{}, y.ValueStruct{})
}

func (b *Builder) Add(key []byte, value y.ValueStruct) error {
	if b.counter >= resultInterval {
		// 每100个 k-v 分割一次
		b.finishBlock()
		b.restarts = append(b.restarts, uint32(b.buf.Len())) // 标记分割块的块偏移
		b.counter = 0
		b.baseKey = []byte{}
		b.baseOffset = uint32(b.buf.Len())
		b.prevOffset = math.MaxUint32
	}
	b.addHelper(key, value)
	return nil
}

// 如果我们...大致（？）达到容量，ReachedCapacity 返回 true
func (b *Builder) ReachedCapacity(cap int64) bool {
	estimateSz := b.buf.Len() + 8 /* empty header */ + 4*len(b.restarts) + 8 // 8 = end of buf offset + len(restarts).
	return int64(estimateSz) > cap
}

// blockIndex 为 Table 生成块索引。它主要是所有分割块的块基偏移量的列表。
func (b *Builder) blockIndex() []byte {
	// Store the end offset, so we know the length of the final block.
	b.restarts = append(b.restarts, uint32(b.buf.Len()))
	// Add 4 because we want to write out number of restarts at the end.
	sz := 4*len(b.restarts) + 4
	out := make([]byte, sz)
	buf := out
	for _, r := range b.restarts {
		binary.BigEndian.PutUint32(buf[:4], r)
		buf = buf[4:]
	}
	// TODO 这里不会覆盖数据吗？
	binary.BigEndian.PutUint32(buf[:4], uint32(len(b.restarts)))
	return out
}

// Finish 通过附加索引来完成 Table。
func (b *Builder) Finish() []byte {
	bf := bbloom.New(float64(b.keyCount), 0.01)
	var klen [2]byte
	key := make([]byte, 1024)
	for {
		// 先解析keysize
		if _, err := b.keyBuf.Read(klen[:]); err == io.EOF {
			break
		} else if err != nil {
			y.Check(err)
		}
		// 根据keysize 读取key
		kl := int(binary.BigEndian.Uint16(klen[:]))
		if cap(key) < kl {
			key = make([]byte, 2*kl)
		}
		key = key[:kl]
		y.Check2(b.keyBuf.Read(key))
		//log.Println("K ", string(key))
		bf.Add(key) // 向 Bloom Filter 添加当前Key
	}

	b.finishBlock() // This will never start a new block.
	index := b.blockIndex()
	b.buf.Write(index)

	// Write bloom filter.
	bdata := bf.JSONMarshal()
	n, err := b.buf.Write(bdata)
	y.Check(err)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(n))
	b.buf.Write(buf[:])

	return b.buf.Bytes()
}
