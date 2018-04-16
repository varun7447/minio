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

package cmd

import (
	"context"
	"io"

	"github.com/minio/minio/cmd/logger"
)

type errIdx struct {
	idx int
	err error
}

type parallelReader struct {
	readers       []*bitrotReader
	dataBlocks    int
	offset        int64
	shardSize     int
	shardFileSize int
	buf           [][]byte
}

func NewParallelReader(readers []*bitrotReader, dataBlocks int, offset int64, fileSize int, blocksize int64) *parallelReader {
	shardSize := ceilFrac(blocksize, int64(dataBlocks))
	shardFileSize := getErasureFileSize(int(blocksize), int64(fileSize), dataBlocks)
	bufs := make([][]byte, len(readers))
	for i := range bufs {
		bufs[i] = make([]byte, shardSize)
	}
	return &parallelReader{
		readers,
		dataBlocks,
		(offset / blocksize) * shardSize,
		int(shardSize),
		int(shardFileSize),
		bufs,
	}
}

func (p *parallelReader) canDecode(buf [][]byte) bool {
	bufCount := 0
	for _, b := range buf {
		if b != nil {
			bufCount++
		}
	}
	return bufCount >= p.dataBlocks
}

func (p *parallelReader) Read() ([][]byte, error) {
	numParallelReaders := p.dataBlocks
	errCh := make(chan errIdx)
	currReaderIndex := 0
	newBuf := make([][]byte, len(p.buf))

	if int(p.offset)+p.shardSize > (p.shardFileSize) {
		p.shardSize = p.shardFileSize - int(p.offset)
		for i := range p.buf {
			p.buf[i] = p.buf[i][:p.shardSize]
		}
	}

	read := func(currReaderIndex int) {
		err := p.readers[currReaderIndex].Read(p.buf[currReaderIndex], p.offset)
		errCh <- errIdx{currReaderIndex, err}
	}

	for numParallelReaders > 0 {
		if p.readers[currReaderIndex] == nil {
			currReaderIndex++
			continue
		}
		go read(currReaderIndex)
		numParallelReaders--
		currReaderIndex++
	}

	for errVal := range errCh {
		if errVal.err == nil {
			newBuf[errVal.idx] = p.buf[errVal.idx]
			if p.canDecode(newBuf) {
				p.offset += int64(p.shardSize)
				return newBuf, nil
			}
			continue
		}
		p.readers[errVal.idx] = nil
		for currReaderIndex < len(p.readers) {
			if p.readers[currReaderIndex] != nil {
				break
			}
			currReaderIndex++
		}

		if currReaderIndex == len(p.readers) {
			break
		}
		go read(currReaderIndex)
		currReaderIndex++
	}

	return nil, errXLReadQuorum
}

func (s ErasureStorage) ReadFile(ctx context.Context, writer io.Writer, readers []*bitrotReader, offset, length, totalLength int64) error {
	if offset < 0 || length < 0 {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}
	if offset+length > totalLength {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}
	if length == 0 {
		return nil
	}

	reader := NewParallelReader(readers, s.dataBlocks, offset, int(totalLength), s.blockSize)

	startBlock := offset / s.blockSize
	endBlock := (offset + length - 1) / s.blockSize

	for block := startBlock; block <= endBlock; block++ {
		var blockOffset, blockLength int64
		switch {
		case startBlock == endBlock:
			blockOffset = offset % s.blockSize
			blockLength = length
		case block == startBlock:
			blockOffset = offset % s.blockSize
			blockLength = s.blockSize - blockOffset
		case block == endBlock:
			blockOffset = 0
			blockLength = (offset + length) % s.blockSize
		default:
			blockOffset = 0
			blockLength = s.blockSize
		}
		bufs, err := reader.Read()
		if err != nil {
			return err
		}
		if err = s.ErasureDecodeDataBlocks(bufs); err != nil {
			return err
		}
		_, err = writeDataBlocks(ctx, writer, bufs, s.dataBlocks, blockOffset, blockLength)
		if err != nil {
			return err
		}
	}
	return nil
}
