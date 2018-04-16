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

type parallelWriter struct {
	writers     []*bitrotWriter
	writeQuorum int
}

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

	return reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, p.writeQuorum)
}

// CreateFile creates a new bitrot encoded file spread over all available disks. CreateFile will create
// the file at the given volume and path. It will read from src until an io.EOF occurs. The given algorithm will
// be used to protect the erasure encoded file.
func (s *ErasureStorage) CreateFile(ctx context.Context, src io.Reader, writers []*bitrotWriter, buf []byte) (total int64, err error) {
	writer := &parallelWriter{
		writers:     writers,
		writeQuorum: s.dataBlocks + 1,
	}
	eof := false
	for {
		var blocks [][]byte
		n, err := io.ReadFull(src, buf)
		switch {
		case err == io.EOF:
			if total != 0 {
				// n = 0, nothing more to be done
				return total, nil
			}
			blocks = make([][]byte, len(s.disks)) // The case where empty object was uploaded
			eof = true
			break
		case err == io.ErrUnexpectedEOF:
			eof = true
			fallthrough
		case err == nil:
			blocks, err = s.ErasureEncode(ctx, buf[:n])
			if err != nil {
				logger.LogIf(ctx, err)
				return 0, err
			}
		default:
			logger.LogIf(ctx, err)
			return 0, err
		}
		if err = writer.Append(ctx, blocks); err != nil {
			return 0, err
		}
		total += int64(n)
		if eof {
			break
		}
	}
	return total, nil
}
