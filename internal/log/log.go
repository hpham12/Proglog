package log

import (
	api "Proglog/api/v1"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// The log consists of a list of segments and a pointer to the active segment
// to append writes to. The directory is where we store the segments
type Log struct {
	mu sync.RWMutex
	Dir string
	Config Config
	activeSegment *segment
	segments []*segment
}


// NewLog will first set defaults for the configs the caller did not specify,
// create a log instance, and set up that instance
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log {
		Dir:	dir,
		Config:	c,
	}

	return l, l.setup()
}

// When a log starts, it is responsible for setting itself up for the segments that
// already exist on disk or, if the log is new and has no exisiting segments, for
// bootstrapping the initial segment
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64

	// fetch the list of segments on disk, parse and sort the base offsets (because
	// we want our slice of segments to be in order from oldest to newest), then create
	// the segments with the newSegments() helper method, which creates a segment for the
	// base offset we pass in
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains duplicates for index and store files so we skip the duplicate
		i++
	}

	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil;
}

// Appends a record to the log. We append the record to the active segment
// Afterward, if the segment is at its max size, then we make a new active segment

// TODO: implement fine-grained locking (lock segment, not the whole log)
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}

	return off, err
}

// Reads the record stored at the given offset
func (l *Log) Read(off uint64) (*api.Record, error) {
	// l.mu.RLock()
	// defer l.mu.RUnlock()

	// Find the segment that contains the given record
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	return s.Read(off)
}

// Close the log by iterating over the segments and close them
func (l *Log) Close() error {
	// l.mu.Lock()
	// defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Closes the log and then removes its data
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Removes the log and then creates a new log to replace it
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// These methods tell us the offset range stored in the log
// When working on supporting replicated, coordinated cluster, we will need 
// this information to know what nodes have the oldest and newest data and what
// nodes are falling behind and need to replicate

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RLock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments) - 1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Removes all segments whose highest offset is lower than lowest to save space
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest + 1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// Returns an io.Reader to read the whole log.
// We will need this capability when we implement coordinate consensus
// and need to support snapshots and restoring a log
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

// The segment stores are wrapped by the originReader type to satify io.Redaer interface
// so we can pass it into the io.MultiReader() call, and to ensure that we begin reading
// from the origin of the store and read its entire file
type originReader struct {
	store *store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.store.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// Creates a new segment, appends that segment to the log's slice of segments,
// and makes the new segment the active segment
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
