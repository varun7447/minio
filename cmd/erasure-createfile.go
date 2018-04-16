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

	"sync"

	"github.com/minio/minio/cmd/logger"
)

// Writes in parallel to bitrotWriters
type parallelWriter struct {
	writers     []*bitrotWriter
	writeQuorum int
}

// Append appends data to bitrotWriters in parallel.
func (p *parallelWriter) Append(ctx context.Context, blocks [][]byte) error {
	var wg sync.WaitGroup
	errs := make([]error, len(p.writers))
	for i, writer := range p.writers {
		if writer == nil {
			errs[i] = errDiskNotFound
			continue
		}

		wg.Add(1)
		go func(i int, writer *bitrotWriter) {
			defer wg.Done()
			errs[i] = writer.Append(blocks[i])
			if errs[i] != nil {
				p.writers[i] = nil
			}
		}(i, writer)
	}
	wg.Wait()

	// If nilCount >= p.writeQuorum, we return nil.
	// HealFile() uses CreateFile with p.writeQuorum=1 to accommodate healing of single disk.
	nilCount := 0
	for _, err := range errs {
		if err == nil {
			nilCount++
		}
	}
	if nilCount >= p.writeQuorum {
		return nil
	}
	return reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, p.writeQuorum)
}

// CreateFile reads from the reader, erasure-encodes the data and writes to the writers.
func (s *ErasureStorage) CreateFile(ctx context.Context, src io.Reader, writers []*bitrotWriter, buf []byte, quorum int) (total int64, err error) {
	writer := &parallelWriter{
		writers:     writers,
		writeQuorum: quorum,
	}
	for {
		eof := false
		var blocks [][]byte
		n, err := io.ReadFull(src, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logger.LogIf(ctx, err)
			return 0, err
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			eof = true
		}
		if n == 0 {
			if total != 0 {
				// Reached EOF, nothing more to be done.
				break
			}
			blocks = make([][]byte, len(writers)) // The case where empty object was uploaded, we create empty shard files.
		} else {
			blocks, err = s.ErasureEncode(ctx, buf[:n])
			if err != nil {
				logger.LogIf(ctx, err)
				return 0, err
			}
		}

		if err = writer.Append(ctx, blocks); err != nil {
			logger.LogIf(ctx, err)
			return 0, err
		}
		total += int64(n)
		if eof {
			break
		}
	}
	return total, nil
}
