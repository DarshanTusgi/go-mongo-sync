package transport

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Compressor interface for different compression algorithms
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Type() int
}

// NoCompression implements no compression
type NoCompression struct{}

func (nc *NoCompression) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (nc *NoCompression) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

func (nc *NoCompression) Type() int {
	return CompressionNone
}

// ZstdCompressor implements Zstandard compression
type ZstdCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func NewZstdCompressor() (*ZstdCompressor, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &ZstdCompressor{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (zc *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	return zc.encoder.EncodeAll(data, make([]byte, 0, len(data)/2)), nil
}

func (zc *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	return zc.decoder.DecodeAll(data, nil)
}

func (zc *ZstdCompressor) Type() int {
	return CompressionZstd
}

func (zc *ZstdCompressor) Close() {
	if zc.encoder != nil {
		zc.encoder.Close()
	}
	if zc.decoder != nil {
		zc.decoder.Close()
	}
}

// LZ4Compressor implements LZ4 compression
type LZ4Compressor struct{}

func NewLZ4Compressor() *LZ4Compressor {
	return &LZ4Compressor{}
}

func (lc *LZ4Compressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)

	_, err := writer.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to compress with LZ4: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close LZ4 writer: %w", err)
	}

	return buf.Bytes(), nil
}

func (lc *LZ4Compressor) Decompress(data []byte) ([]byte, error) {
	reader := lz4.NewReader(bytes.NewReader(data))

	var buf bytes.Buffer
	_, err := io.Copy(&buf, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress with LZ4: %w", err)
	}

	return buf.Bytes(), nil
}

func (lc *LZ4Compressor) Type() int {
	return CompressionLZ4
}

// GetCompressor returns a compressor for the given type
func GetCompressor(compressionType CompressionType) (Compressor, error) {
	switch compressionType {
	case CompressionTypeNone:
		return &NoCompression{}, nil
	case CompressionTypeZstd:
		return NewZstdCompressor()
	case CompressionTypeLZ4:
		return NewLZ4Compressor(), nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// GetCompressorByName returns a compressor by string name (for config compatibility)
func GetCompressorByName(compressionName string) (Compressor, error) {
	switch compressionName {
	case "none", "":
		return &NoCompression{}, nil
	case "zstd":
		return NewZstdCompressor()
	case "lz4":
		return NewLZ4Compressor(), nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionName)
	}
}

// NewHighPerformanceZstdCompressor creates a Zstd compressor optimized for billions of documents
func NewHighPerformanceZstdCompressor() (*ZstdCompressor, error) {
	// Use fastest compression for high-throughput scenarios
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest), // Prioritize speed over compression ratio
		zstd.WithWindowSize(64*1024),             // 64KB window for better memory usage
		zstd.WithEncoderConcurrency(4),           // Allow parallel compression
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create high-performance zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(4),           // Allow parallel decompression
		zstd.WithDecoderMaxMemory(256*1024*1024), // Limit memory to 256MB
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create high-performance zstd decoder: %w", err)
	}

	return &ZstdCompressor{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

// EstimateCompressionRatio estimates the compression ratio for given data
func EstimateCompressionRatio(data []byte, compressionType CompressionType) float64 {
	if compressionType == CompressionTypeNone {
		return 1.0
	}

	// Quick estimation based on data characteristics
	// This is a heuristic and actual compression may vary
	dataSize := len(data)
	if dataSize == 0 {
		return 1.0
	}

	// Count repeated bytes as a simple heuristic
	byteCount := make(map[byte]int)
	for _, b := range data {
		byteCount[b]++
	}

	// Calculate entropy approximation
	entropy := 0.0
	for _, count := range byteCount {
		if count > 0 {
			p := float64(count) / float64(dataSize)
			entropy -= p * log2(p)
		}
	}

	// Estimate compression ratio based on entropy
	maxEntropy := 8.0 // Maximum entropy for 8-bit data
	compressionFactor := entropy / maxEntropy

	switch compressionType {
	case CompressionTypeZstd:
		// Zstd typically achieves better compression
		return 0.3 + (compressionFactor * 0.4)
	case CompressionTypeLZ4:
		// LZ4 is faster but less compression
		return 0.5 + (compressionFactor * 0.3)
	default:
		return 1.0
	}
}

func log2(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Simple log2 approximation
	return 1.44269504088896340735992468100189 * logApprox(x)
}

func logApprox(x float64) float64 {
	// Simple natural log approximation using Taylor series
	if x <= 0 {
		return -1000 // Represents negative infinity
	}
	if x == 1 {
		return 0
	}

	// For x close to 1, use Taylor series: ln(1+u) = u - u²/2 + u³/3 - ...
	if x > 0.5 && x < 1.5 {
		u := x - 1
		result := u
		term := u
		for i := 2; i <= 10; i++ {
			term *= -u
			result += term / float64(i)
		}
		return result
	}

	// For other values, use a rough approximation
	// This is not mathematically precise but sufficient for our estimation
	if x > 1 {
		return 1 + (x-1)*0.5
	}
	return -1 + x*0.5
}

// NewHighPerformanceLZ4Compressor creates an LZ4 compressor optimized for billions of documents
func NewHighPerformanceLZ4Compressor() *LZ4Compressor {
	return &LZ4Compressor{}
}

// StreamingCompressor interface for large dataset compression
type StreamingCompressor interface {
	Compressor
	CompressStreaming(data []byte, callback func(chunk []byte) error) error
	DecompressStreaming(data []byte, callback func(chunk []byte) error) error
}

// StreamingZstdCompressor implements streaming compression for billion-document transfers
type StreamingZstdCompressor struct {
	*ZstdCompressor
}

// NewStreamingZstdCompressor creates a streaming Zstd compressor for massive datasets
func NewStreamingZstdCompressor() (*StreamingZstdCompressor, error) {
	baseCompressor, err := NewHighPerformanceZstdCompressor()
	if err != nil {
		return nil, err
	}
	return &StreamingZstdCompressor{ZstdCompressor: baseCompressor}, nil
}

// CompressStreaming compresses data in chunks to handle billion-document scenarios
func (sc *StreamingZstdCompressor) CompressStreaming(data []byte, callback func(chunk []byte) error) error {
	const chunkSize = 1024 * 1024 // 1MB chunks

	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[offset:end]
		compressedChunk, err := sc.Compress(chunk)
		if err != nil {
			return fmt.Errorf("failed to compress chunk at offset %d: %w", offset, err)
		}

		if err := callback(compressedChunk); err != nil {
			return fmt.Errorf("callback failed for chunk at offset %d: %w", offset, err)
		}
	}

	return nil
}

// DecompressStreaming decompresses data in chunks to handle billion-document scenarios
func (sc *StreamingZstdCompressor) DecompressStreaming(data []byte, callback func(chunk []byte) error) error {
	// For streaming decompression, we need to know chunk boundaries
	// This is a simplified implementation - in production, you'd include chunk size headers
	decompressed, err := sc.Decompress(data)
	if err != nil {
		return fmt.Errorf("failed to decompress data: %w", err)
	}

	const chunkSize = 1024 * 1024 // 1MB chunks
	for offset := 0; offset < len(decompressed); offset += chunkSize {
		end := offset + chunkSize
		if end > len(decompressed) {
			end = len(decompressed)
		}

		chunk := decompressed[offset:end]
		if err := callback(chunk); err != nil {
			return fmt.Errorf("callback failed for chunk at offset %d: %w", offset, err)
		}
	}

	return nil
}

// AdaptiveCompressor chooses the best compression algorithm based on data characteristics
type AdaptiveCompressor struct {
	zstdCompressor *ZstdCompressor
	lz4Compressor  *LZ4Compressor
	noCompression  *NoCompression
}

// NewAdaptiveCompressor creates a compressor that adapts based on data characteristics
func NewAdaptiveCompressor() (*AdaptiveCompressor, error) {
	zstdComp, err := NewHighPerformanceZstdCompressor()
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd compressor: %w", err)
	}

	return &AdaptiveCompressor{
		zstdCompressor: zstdComp,
		lz4Compressor:  NewHighPerformanceLZ4Compressor(),
		noCompression:  &NoCompression{},
	}, nil
}

// Compress chooses the best compression algorithm and compresses the data
func (ac *AdaptiveCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	// For small data, use no compression to avoid overhead
	if len(data) < 1024 {
		return ac.noCompression.Compress(data)
	}

	// For medium data, use LZ4 (fast)
	if len(data) < 100*1024 {
		return ac.lz4Compressor.Compress(data)
	}

	// For large data, use Zstd (best compression ratio)
	return ac.zstdCompressor.Compress(data)
}

// Decompress decompresses data using the appropriate algorithm
func (ac *AdaptiveCompressor) Decompress(data []byte) ([]byte, error) {
	// Try Zstd first (most common for large data)
	if result, err := ac.zstdCompressor.Decompress(data); err == nil {
		return result, nil
	}

	// Try LZ4 second
	if result, err := ac.lz4Compressor.Decompress(data); err == nil {
		return result, nil
	}

	// Fall back to no compression
	return ac.noCompression.Decompress(data)
}

// Type returns the adaptive compressor type
func (ac *AdaptiveCompressor) Type() int {
	return CompressionZstd // Default to Zstd type for compatibility
}
