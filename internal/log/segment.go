package log

import (
	"fmt"
	"os"
	"path"
	api "Proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// The segment wraps the index and store types to coordinate operations across
// the two. For example, when the log appends a record to the active segment,
// the segment needs to write the data to its store and add a new entry in the
// index. Similarly for reads, the segment needs to look up the entry from the
// index and then fetch the data from the store.

// Our segment needs to call its store and index files, so we keep pointers to
// those in the first two fields

// We need the next and base offsets to know what offset to append new records 
// under and to calculate the relative offsets for the index entries

// We put the config on the segment so we can compare the store file and index sizes
// to the configured limits, which lets us know when the segment is maxed out
type segment struct {
	store					*store
	index					*index
	baseOffset, nextOffset	uint64
	config					Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment {
		baseOffset: baseOffset,
		config: c,
	}

	var err error

	// initialize store file

	// Create file if not exists and only allow appending when writing
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// initialize index file
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset;
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1;
	}

	return s, nil;
}

// Writes the record to the segment and returns the newly appended record's offset
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// Appends data to the store
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// Adds index entry
	if err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset - uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Returns the orecord for the given offset
func (s *segment) Read(off uint64) (*api.Record, error) {
	// Look up the position of the record in store file
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// Returns whether the segment has reached its max size, either by writing too
// much to the store or the index
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
	s.index.size >= s.config.Segment.MaxIndexBytes
}

// Closes the segment and removes the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// returns the nearest and lesser multiple of k in j
// for example nearestMultiple(9, 4) = 8
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}

	return ((j - k + 1) / k) * k
}
