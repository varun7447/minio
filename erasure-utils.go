/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"crypto/sha512"
	"hash"
	"io"

	"bytes"

	"github.com/klauspost/reedsolomon"
)

// newHashWriters - inititialize a slice of hashes for the disk count.
func newHashWriters(diskCount int) []hash.Hash {
	hashWriters := make([]hash.Hash, diskCount)
	for index := range hashWriters {
		hashWriters[index] = newHash("sha512")
	}
	return hashWriters
}

// newHash - gives you a newly allocated hash depending on the input algorithm.
func newHash(algo string) hash.Hash {
	switch algo {
	case "sha512":
		return sha512.New()
	// Add new hashes here.
	default:
		return sha512.New()
	}
}

func hashSum(disk StorageAPI, volume, path string, writer hash.Hash) ([]byte, error) {
	startOffset := int64(0)
	// Read until io.EOF.
	for {
		buf := make([]byte, blockSizeV1)
		n, err := disk.ReadFile(volume, path, startOffset, buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		writer.Write(buf[:n])
		startOffset += n
	}
	return writer.Sum(nil), nil
}

// getDataBlocks - fetches the data block only part of the input encoded blocks.
func getDataBlocks(enBlocks [][]byte, dataBlocks int, curBlockSize int, writer io.Writer) (err error) {
	if len(enBlocks) < dataBlocks {
		return reedsolomon.ErrTooFewShards
	}
	size := 0
	blocks := enBlocks[:dataBlocks]
	for _, block := range blocks {
		size += len(block)
	}
	if size < curBlockSize {
		return reedsolomon.ErrShortData
	}

	write := curBlockSize
	for _, block := range blocks {
		if write < len(block) {
			// data = append(data, block[:write]...)
			io.Copy(writer, bytes.NewReader(block[:write]))
			return nil
		}
		// data = append(data, block...)
		io.Copy(writer, bytes.NewReader(block))
		write -= len(block)
	}
	return nil
}

// checkBlockSize return the size of a single block.
// The first non-zero size is returned,
// or 0 if all blocks are size 0.
func checkBlockSize(blocks [][]byte) int {
	for _, block := range blocks {
		if len(block) != 0 {
			return len(block)
		}
	}
	return 0
}

// calculate the blockSize based on input length and total number of
// data blocks.
func getEncodedBlockLen(inputLen int64, dataBlocks int) (curEncBlockSize int64) {
	curEncBlockSize = (inputLen + int64(dataBlocks) - 1) / int64(dataBlocks)
	return curEncBlockSize
}
