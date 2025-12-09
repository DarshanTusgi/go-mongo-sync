package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Segment represents a single WAL segment file
type Segment struct {
	path       string
	file       *os.File
	writer     *bufio.Writer
	size       int64
	segmentNum uint64
	mu         sync.Mutex
	closed     bool
}

// NewSegment creates a new WAL segment
func NewSegment(directory string, segmentNum uint64) (*Segment, error) {
	path := fmt.Sprintf("%s/%020d.wal", directory, segmentNum)
	
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}
	
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat segment file: %w", err)
	}
	
	return &Segment{
		path:       path,
		file:       file,
		writer:     bufio.NewWriterSize(file, 256*1024), // 256KB buffer (like Kafka)
		size:       info.Size(),
		segmentNum: segmentNum,
		closed:     false,
	}, nil
}

// Append writes an entry to the segment with checksum (RocksDB-style record format)
func (s *Segment) Append(entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return ErrWALClosed
	}
	
	// Serialize entry to wire format
	data, err := s.serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("serialize entry: %w", err)
	}
	
	// Write to buffered writer
	if _, err := s.writer.Write(data); err != nil {
		return fmt.Errorf("write to segment: %w", err)
	}
	
	s.size += int64(len(data))
	
	return nil
}

// Sync flushes buffered writes and calls fsync (like Kafka)
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return ErrWALClosed
	}
	
	// Flush buffer
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("flush writer: %w", err)
	}
	
	// Force kernel to write to disk
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	
	return nil
}

// Size returns current segment size
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Close closes the segment
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.closed {
		return nil
	}
	
	s.closed = true
	
	// Flush and sync before close
	if err := s.writer.Flush(); err != nil {
		return err
	}
	
	if err := s.file.Sync(); err != nil {
		return err
	}
	
	return s.file.Close()
}

// serializeEntry encodes entry to wire format with CRC32C checksum
// Format (like RocksDB):
// [CRC32: 4 bytes][EntryID: 8 bytes][Timestamp: 8 bytes][RecordType: 1 byte][Status: 1 byte]
// [DatabaseLen: 2 bytes][Database: var][CollectionLen: 2 bytes][Collection: var]
// [DocumentsLen: 4 bytes][Documents: var]
func (s *Segment) serializeEntry(entry *Entry) ([]byte, error) {
	// Calculate payload size
	payloadSize := 8 + 8 + 1 + 1 + 2 + len(entry.Database) + 2 + len(entry.Collection) + 4 + len(entry.Documents)
	
	// Allocate buffer (4 bytes CRC + payload)
	buf := make([]byte, 4+payloadSize)
	offset := 4 // Skip CRC for now
	
	// Write header
	binary.LittleEndian.PutUint64(buf[offset:], entry.EntryID)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(entry.Timestamp))
	offset += 8
	buf[offset] = byte(entry.RecordType)
	offset++
	buf[offset] = byte(entry.Status)
	offset++
	
	// Write database
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(entry.Database)))
	offset += 2
	copy(buf[offset:], entry.Database)
	offset += len(entry.Database)
	
	// Write collection
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(entry.Collection)))
	offset += 2
	copy(buf[offset:], entry.Collection)
	offset += len(entry.Collection)
	
	// Write documents
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(entry.Documents)))
	offset += 4
	copy(buf[offset:], entry.Documents)
	
	// Calculate CRC32C of payload (like Kafka, RocksDB)
	crc := crc32.Checksum(buf[4:], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	
	return buf, nil
}

// ReadAll reads all entries from the segment
func ReadSegment(path string) ([]*Entry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}
	defer file.Close()
	
	reader := bufio.NewReader(file)
	var entries []*Entry
	
	for {
		entry, err := readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, fmt.Errorf("read entry: %w", err)
		}
		entries = append(entries, entry)
	}
	
	return entries, nil
}

// readEntry reads a single entry from the reader with checksum validation
func readEntry(reader *bufio.Reader) (*Entry, error) {
	// Read CRC (4 bytes)
	crcBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, crcBytes); err != nil {
		return nil, err
	}
	expectedCRC := binary.LittleEndian.Uint32(crcBytes)
	
	// Read fixed header (8 + 8 + 1 + 1 = 18 bytes)
	header := make([]byte, 18)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}
	
	entry := &Entry{}
	offset := 0
	
	// Parse header
	entry.EntryID = binary.LittleEndian.Uint64(header[offset:])
	offset += 8
	entry.Timestamp = int64(binary.LittleEndian.Uint64(header[offset:]))
	offset += 8
	entry.RecordType = RecordType(header[offset])
	offset++
	entry.Status = EntryStatus(header[offset])
	offset++
	
	// Read database length and value
	dbLenBytes := make([]byte, 2)
	if _, err := io.ReadFull(reader, dbLenBytes); err != nil {
		return nil, err
	}
	dbLen := binary.LittleEndian.Uint16(dbLenBytes)
	
	dbBytes := make([]byte, dbLen)
	if _, err := io.ReadFull(reader, dbBytes); err != nil {
		return nil, err
	}
	entry.Database = string(dbBytes)
	
	// Read collection length and value
	collLenBytes := make([]byte, 2)
	if _, err := io.ReadFull(reader, collLenBytes); err != nil {
		return nil, err
	}
	collLen := binary.LittleEndian.Uint16(collLenBytes)
	
	collBytes := make([]byte, collLen)
	if _, err := io.ReadFull(reader, collBytes); err != nil {
		return nil, err
	}
	entry.Collection = string(collBytes)
	
	// Read documents length and value
	docsLenBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, docsLenBytes); err != nil {
		return nil, err
	}
	docsLen := binary.LittleEndian.Uint32(docsLenBytes)
	
	docsBytes := make([]byte, docsLen)
	if _, err := io.ReadFull(reader, docsBytes); err != nil {
		return nil, err
	}
	entry.Documents = docsBytes
	
	// Verify CRC32C checksum
	payload := make([]byte, 0, 18+2+int(dbLen)+2+int(collLen)+4+int(docsLen))
	payload = append(payload, header...)
	payload = append(payload, dbLenBytes...)
	payload = append(payload, dbBytes...)
	payload = append(payload, collLenBytes...)
	payload = append(payload, collBytes...)
	payload = append(payload, docsLenBytes...)
	payload = append(payload, docsBytes...)
	
	actualCRC := crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	if actualCRC != expectedCRC {
		return nil, ErrCorruptedEntry
	}
	
	return entry, nil
}
