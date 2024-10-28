package log

// An index entry contains 2 fields: The record's offset and its position in the store file
import (
	"fmt"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// define the number of bytes that make up each index entry
var (
	offWidth uint64 = 4 //width of the field holding record offset
	posWidth uint64 = 8 //width of the field holding record position
	entWidth		= offWidth + posWidth
)

type index struct {
	file *os.File // persisted file
	mmap gommap.MMap // memory-mapped file
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index {
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Takes in an offset and returns and associated record's position in the store
// The given offset is relative to the segment's base offset
func (i *index) Read(in int64) (off uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		off = uint32((i.size / entWidth) - 1)
	} else {
		off = uint32(in)
	}
	pos = uint64(off) * entWidth
	if i.size < pos + entWidth {
		return 0, 0, io.EOF
	}
	off = encodingStyle.Uint32(i.mmap[pos : pos + offWidth])
	pos = encodingStyle.Uint64(i.mmap[pos + offWidth : pos + entWidth])
	return off, pos, nil
}

// Method to append the given offset and position to the index
func (i *index) Write(off uint32, pos uint64) error {
	fmt.Printf("current index size %d", i.size)
	if uint64(len(i.mmap)) < i.size + entWidth {
		return io.EOF;
	}
	encodingStyle.PutUint32(i.mmap[i.size : i.size+offWidth], off)
	encodingStyle.PutUint64(i.mmap[i.size+offWidth : i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil;
}

// Method to return the index's file path
func (i *index) Name() string {
	return i.file.Name()
}
