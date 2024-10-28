package log

import (
	// "io"
	"os"
	"testing"
	"github.com/stretchr/testify/require"
	api "Proglog/api/v1"
)

var (
	record_val_1 = []byte("Hello world 1")
	record_val_2 = []byte("Hello world 2")
)

func TestSegment(t *testing.T) {
	c := Config{}

	c.Segment.MaxIndexBytes = 1024
	c.Segment.MaxStoreBytes = 2048

	segment, err := newSegment(os.TempDir(), 0, c)
	require.NoError(t, err)

	// test append
	testSegmentAppend(t, segment)

	// test read
	testSegmentRead(t, segment)


	segment.Remove()
}

func testSegmentAppend(t *testing.T, s *segment) {
	offset, err := s.Append(&api.Record{Value: record_val_1})
	require.NoError(t, err)
	require.EqualValues(t, 0, offset)

	offset, err = s.Append(&api.Record{Value: record_val_2})
	require.NoError(t, err)
	require.EqualValues(t, 1, offset)
}

func testSegmentRead(t *testing.T, s *segment) {
	record, err := s.Read(0)
	require.NoError(t, err)
	require.EqualValues(t, record_val_1, record.Value)
}
