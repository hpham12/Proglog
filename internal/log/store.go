package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// encoding style used in persistence of record sizes and index entries
	encodingStyle = binary.BigEndian
)

const (
	// number of bytes used to store the record's length
	lenWidth = 8
)

// store struct is a simple wrapper around a file with 2 APIs
// to append and read bytes to and from the file
type store struct {
	*os.File
	mu             sync.Mutex
	bufferedWriter *bufio.Writer
	size           uint64
}

func newStore(file *os.File) (*store, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fileInfo.Size())
	return &store{
		File:           file,
		size:           size,
		bufferedWriter: bufio.NewWriter(file),
	}, nil
}

func (store *store) Append(data []byte) (n uint64, pos uint64, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	position := store.size
	if err := binary.Write(store.bufferedWriter, encodingStyle, uint64(len(data))); err != nil {
		return 0, 0, err
	}

	// write to the buffered writer instead of directly to the file
	// to reduce # of systems calls and improve performance
	bytesWritten, err := store.bufferedWriter.Write(data)
	if err != nil {
		return 0, 0, err
	}
	bytesWritten += lenWidth
	store.size += uint64(bytesWritten)
	return uint64(bytesWritten), position, nil
}

func (store *store) Read(position uint64) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.bufferedWriter.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := store.File.ReadAt(size, int64(position)); err != nil {
		return nil, err
	}
	b := make([]byte, encodingStyle.Uint64(size))
	if _, err := store.File.ReadAt(b, int64(position+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (store *store) ReadAt(p []byte, offset int64) (int, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.bufferedWriter.Flush(); err != nil {
		return 0, err
	}
	return store.File.ReadAt(p, offset)
}

func (store *store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	err := store.bufferedWriter.Flush()
	if err != nil {
		return err
	}
	return store.File.Close()
}
