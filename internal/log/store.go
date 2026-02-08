package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	// a header is appended to the buffer
	// then the actual contents of the record is written
	// because while reading, we'll need to read haeder first, then the contents
	// currently, the header is uint64 (hence 8 bytes) showing length of the record
	headerSizeBytes = 8
)

// store is just a wrapper around os.File
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(record []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// write to the end of the file
	pos := s.size

	// write length of the record (8 bytes) before the content
	if err := binary.Write(s.buf, enc, uint64(len(record))); err != nil {
		return 0, 0, err
	}

	// then write the actual content
	w, err := s.buf.Write(record)
	if err != nil {
		return 0, 0, err
	}

	// total written bytes = bytesWritten + header size
	w += headerSizeBytes
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush the writer buffer, in case we’re about to try to read a record
	// that the buffer hasn’t flushed to disk yet
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// read the length of the content
	// to know how many bytes we need to read
	header := make([]byte, headerSizeBytes)
	if _, err := s.File.ReadAt(header, int64(pos)); err != nil {
		return nil, err
	}

	// read the actual contents
	contents := make([]byte, enc.Uint64(header))
	if _, err := s.File.ReadAt(contents, int64(pos+headerSizeBytes)); err != nil {
		return nil, err
	}

	return contents, nil
}

func (s *store) ReadAt(b []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(b, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
