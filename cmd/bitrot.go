package cmd

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/highwayhash"
	"github.com/minio/minio/cmd/logger"
	"golang.org/x/crypto/blake2b"
)

// magic HH-256 key as HH-256 hash of the first 100 decimals of π as utf-8 string with a zero key.
var magicHighwayHash256Key = []byte("\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0")

// BitrotAlgorithm specifies a algorithm used for bitrot protection.
type BitrotAlgorithm uint

const (
	// SHA256 represents the SHA-256 hash function
	SHA256 BitrotAlgorithm = 1 + iota
	// HighwayHash256 represents the HighwayHash-256 hash function
	HighwayHash256
	// HighwayHash256 Granular
	HighwayHash256G
	// BLAKE2b512 represents the BLAKE2b-256 hash function
	BLAKE2b512
)

// DefaultBitrotAlgorithm is the default algorithm used for bitrot protection.
const (
	DefaultBitrotAlgorithm = HighwayHash256G
	DefaultBitrotBlockSize = 32 * humanize.MiByte
)

var bitrotAlgorithms = map[BitrotAlgorithm]string{
	SHA256:          "sha256",
	BLAKE2b512:      "blake2b",
	HighwayHash256:  "highwayhash256",
	HighwayHash256G: "highwayhash256G",
}

// New returns a new hash.Hash calculating the given bitrot algorithm.
// New logs error and exits if the algorithm is not supported or not
// linked into the binary.
func (a BitrotAlgorithm) New() hash.Hash {
	switch a {
	case SHA256:
		return sha256.New()
	case BLAKE2b512:
		b2, _ := blake2b.New512(nil) // New512 never returns an error if the key is nil
		return b2
	case HighwayHash256, HighwayHash256G:
		hh, _ := highwayhash.New(magicHighwayHash256Key) // New will never return error since key is 256 bit
		return hh
	}
	logger.CriticalIf(context.Background(), errors.New("Unsupported bitrot algorithm"))
	return nil
}

// Available reports whether the given algorihm is a supported and linked into the binary.
func (a BitrotAlgorithm) Available() bool {
	_, ok := bitrotAlgorithms[a]
	return ok
}

// String returns the string identifier for a given bitrot algorithm.
// If the algorithm is not supported String panics.
func (a BitrotAlgorithm) String() string {
	name, ok := bitrotAlgorithms[a]
	if !ok {
		logger.CriticalIf(context.Background(), errors.New("Unsupported bitrot algorithm"))
	}
	return name
}

// NewBitrotVerifier returns a new BitrotVerifier implementing the given algorithm.
func NewBitrotVerifier(algorithm BitrotAlgorithm, checksum []byte) *BitrotVerifier {
	return &BitrotVerifier{algorithm, checksum}
}

// BitrotVerifier can be used to verify protected data.
type BitrotVerifier struct {
	algorithm BitrotAlgorithm
	sum       []byte
}

// BitrotAlgorithmFromString returns a bitrot algorithm from the given string representation.
// It returns 0 if the string representation does not match any supported algorithm.
// The zero value of a bitrot algorithm is never supported.
func BitrotAlgorithmFromString(s string) (a BitrotAlgorithm) {
	for alg, name := range bitrotAlgorithms {
		if name == s {
			return alg
		}
	}
	return
}

type bitrotReader struct {
	disk        StorageAPI
	volume      string
	filePath    string
	algo        BitrotAlgorithm
	blockSize   int
	sums        [][]byte
	fileOffset  int64
	fileLength  int64
	blockBuf    []byte
	blockOffset int
}

func NewBitrotReader(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm, blockSize int, sums [][]byte, fileLength int64) *bitrotReader {
	return &bitrotReader{
		disk:        disk,
		volume:      volume,
		filePath:    filePath,
		algo:        algo,
		blockSize:   blockSize,
		sums:        sums,
		fileLength:  fileLength,
		blockBuf:    make([]byte, blockSize),
		blockOffset: 0,
	}
}

func (b *bitrotReader) Read(buf []byte, offset int64) error {
	ctx := context.Background()
	if b.fileOffset == 0 {
		b.fileOffset = offset
	}
	if offset != b.fileOffset {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}
	if offset >= b.fileLength {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}
	var remainingBuf []byte
	if len(buf) > b.blockSize-b.blockOffset {
		bufLen := b.blockSize - b.blockOffset
		remainingBuf = buf[bufLen:]
		buf = buf[:bufLen]
	}
	if b.blockOffset == 0 {
		idx := offset / int64(b.blockSize)
		if int(idx) >= len(b.sums) {
			logger.LogIf(ctx, errUnexpected)
			return errUnexpected
		}
		verifier := NewBitrotVerifier(b.algo, b.sums[idx])
		if int(idx) == len(b.sums)-1 {
			b.blockBuf = b.blockBuf[:(b.fileLength - (idx * int64(b.blockSize)))]
		}
		_, err := b.disk.ReadFile(b.volume, b.filePath, idx*int64(b.blockSize), b.blockBuf, verifier)
		if err != nil {
			logger.LogIf(ctx, err)
			return err
		}
		b.blockOffset = int(offset) % b.blockSize
	}
	n := copy(buf, b.blockBuf[b.blockOffset:])
	if n != len(buf) {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}
	b.blockOffset += n
	b.fileOffset += int64(n)
	if b.blockOffset == len(b.blockBuf) {
		b.blockOffset = 0
	}
	if remainingBuf != nil {
		return b.Read(remainingBuf, b.fileOffset)
	}
	return nil
}

type bitrotWriter struct {
	disk        StorageAPI
	volume      string
	filePath    string
	blockSize   int
	h           hash.Hash
	sums        [][]byte
	blockOffset int
}

func NewBitrotWriter(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm, blockSize int) *bitrotWriter {
	return &bitrotWriter{
		disk:      disk,
		volume:    volume,
		filePath:  filePath,
		blockSize: blockSize,
		h:         algo.New(),
	}
}

func (b *bitrotWriter) Append(buf []byte) error {
	var remainingBuf []byte

	if b.blockOffset+len(buf) > b.blockSize {
		bufLen := b.blockSize - b.blockOffset
		remainingBuf = buf[bufLen:]
		buf = buf[:bufLen]
	}

	n, err := b.h.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return errUnexpected
	}
	if err = b.disk.AppendFile(b.volume, b.filePath, buf); err != nil {
		return err
	}
	b.blockOffset += len(buf)
	if b.blockOffset == b.blockSize {
		b.sums = append(b.sums, b.h.Sum(nil))
		b.h.Reset()
		b.blockOffset = 0
	}
	if remainingBuf != nil {
		return b.Append(remainingBuf)
	}
	return nil
}

func (b *bitrotWriter) Close() [][]byte {
	if b.blockOffset != 0 {
		b.sums = append(b.sums, b.h.Sum(nil))
		b.blockOffset = 0
	}
	return b.sums
}
