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
	"errors"
	"io"
)

func copyN(writer io.Writer, disk StorageAPI, volume string, path string, offset int64, length int64) error {
	buf := make([]byte, 32*1024)

	// Read into writer until length.
	for length > 0 {
		n, err := disk.ReadFile(volume, path, offset, buf)
		if n > 0 {
			var m int
			m, err = writer.Write(buf[:n])
			if err != nil {
				return err
			}

			// Decrement the length.
			length -= int64(m)

			// Progress the offset.
			offset += int64(m)
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
	}

	// Success.
	return nil
}

// copyBuffer - copies from disk, volume, path to input writer until either EOF
// is reached at volume, path or an error occurs. A success copyBuffer returns
// err == nil, not err == EOF. Because copyBuffer is defined to read from path
// until EOF. It does not treat an EOF from ReadFile an error to be reported.
// Additionally copyBuffer stages through the provided buffer; otherwise if it
// has zero length, returns error.
func copyBuffer(writer io.Writer, disk StorageAPI, volume string, path string, buf []byte) error {
	// Error condition of zero length buffer.
	if buf != nil && len(buf) == 0 {
		return errors.New("empty buffer in readBuffer")
	}

	// Starting offset for Reading the file.
	startOffset := int64(0)

	// Read until io.EOF.
	for {
		n, err := disk.ReadFile(volume, path, startOffset, buf)
		if n > 0 {
			var m int
			m, err = writer.Write(buf[:n])
			if err != nil {
				return err
			}
			if int64(m) != n {
				return io.ErrShortWrite
			}
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// Progress the offset.
		startOffset += n
	}

	// Success.
	return nil
}
