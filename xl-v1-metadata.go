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
	"bytes"
	"encoding/json"
	"io"
	"path"
	"sort"
	"sync"
	"time"
)

// Erasure block size.
const (
	erasureBlockSize          = 4 * 1024 * 1024 // 4MiB.
	erasureAlgorithmKlauspost = "klauspost/reedsolomon/vandermonde"
	erasureAlgorithmISAL      = "isa-l/reedsolomon/cauchy"
)

// objectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type objectPartInfo struct {
	Number int    `json:"number"`
	Name   string `json:"name"`
	ETag   string `json:"etag"`
	Size   int64  `json:"size"`
}

// A xlMetaV1 represents a metadata header mapping keys to sets of values.
type xlMetaV1 struct {
	Version string `json:"version"`
	Format  string `json:"format"`
	Stat    struct {
		Size    int64     `json:"size"`
		ModTime time.Time `json:"modTime"`
		Version int64     `json:"version"`
	} `json:"stat"`
	Erasure struct {
		Algorithm    string `json:"algorithm"`
		DataBlocks   int    `json:"data"`
		ParityBlocks int    `json:"parity"`
		BlockSize    int64  `json:"blockSize"`
		Index        int    `json:"index"`
		Distribution []int  `json:"distribution"`
		Checksum     []struct {
			Name      string `json:"name"`
			Algorithm string `json:"algorithm"`
			Hash      string `json:"hash"`
		} `json:"checksum"`
	} `json:"erasure"`
	Minio struct {
		Release string `json:"release"`
	} `json:"minio"`
	Meta  map[string]string `json:"meta"`
	Parts []objectPartInfo  `json:"parts,omitempty"`
}

// ReadFrom - read from implements io.ReaderFrom interface for
// unmarshalling xlMetaV1.
func (m *xlMetaV1) ReadFrom(reader io.Reader) (n int64, err error) {
	var buffer bytes.Buffer
	n, err = buffer.ReadFrom(reader)
	if err != nil {
		return 0, err
	}
	err = json.Unmarshal(buffer.Bytes(), m)
	return n, err
}

// WriteTo - write to implements io.WriterTo interface for marshalling xlMetaV1.
func (m xlMetaV1) WriteTo(writer io.Writer) (n int64, err error) {
	metadataBytes, err := json.Marshal(&m)
	if err != nil {
		return 0, err
	}
	p, err := writer.Write(metadataBytes)
	return int64(p), err
}

// byPartName is a collection satisfying sort.Interface.
type byPartNumber []objectPartInfo

func (t byPartNumber) Len() int           { return len(t) }
func (t byPartNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t byPartNumber) Less(i, j int) bool { return t[i].Number < t[j].Number }

// SearchObjectPart - searches for part name and etag, returns the
// index if found.
func (m xlMetaV1) SearchObjectPart(number int) int {
	for i, part := range m.Parts {
		if number == part.Number {
			return i
		}
	}
	return -1
}

// AddObjectPart - add a new object part in order.
func (m *xlMetaV1) AddObjectPart(number int, name string, etag string, size int64) {
	partInfo := objectPartInfo{
		Number: number,
		Name:   name,
		ETag:   etag,
		Size:   size,
	}
	for i, part := range m.Parts {
		if number == part.Number {
			m.Parts[i] = partInfo
			return
		}
	}
	m.Parts = append(m.Parts, partInfo)
	sort.Sort(byPartNumber(m.Parts))
}

// getPartIndexOffset - given an offset for the whole object, return the part and offset in that part.
func (m xlMetaV1) getPartIndexOffset(offset int64) (partIndex int, partOffset int64, err error) {
	partOffset = offset
	for i, part := range m.Parts {
		partIndex = i
		if part.Size == 0 {
			return partIndex, partOffset, nil
		}
		if partOffset < part.Size {
			return partIndex, partOffset, nil
		}
		partOffset -= part.Size
	}
	// Offset beyond the size of the object
	err = errUnexpected
	return 0, 0, err
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (xl xlObjects) parentDirIsObject(bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." {
			return false
		}
		if xl.isObject(bucket, p) {
			// If there is already a file at prefix "p" return error.
			return true
		}
		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

func (xl xlObjects) isObject(bucket, prefix string) bool {
	// Create errs and volInfo slices of storageDisks size.
	var errs = make([]error, len(xl.storageDisks))

	// Allocate a new waitgroup.
	var wg = &sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		// Stat file on all the disks in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			_, err := disk.StatFile(bucket, path.Join(prefix, xlMetaJSONFile))
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the Stat operations to finish.
	wg.Wait()

	var errFileNotFoundCount int
	for _, err := range errs {
		if err != nil {
			if err == errFileNotFound {
				errFileNotFoundCount++
				// If we have errors with file not found greater than allowed read
				// quorum we return err as errFileNotFound.
				if errFileNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
					return false
				}
				continue
			}
			errorIf(err, "Unable to access file "+path.Join(bucket, prefix))
			return false
		}
	}
	return true
}

// statPart - stat a part file.
func (xl xlObjects) statPart(bucket, objectPart string) (fileInfo FileInfo, err error) {
	// Count for errors encountered.
	var xlJSONErrCount = 0

	// Return the first success entry based on the selected random disk.
	for xlJSONErrCount < len(xl.storageDisks) {
		// Choose a random disk on each attempt, do not hit the same disk all the time.
		disk := xl.getRandomDisk() // Pick a random disk.
		fileInfo, err = disk.StatFile(bucket, objectPart)
		if err == nil {
			return fileInfo, nil
		}
		xlJSONErrCount++ // Update error count.
	}
	return FileInfo{}, err
}

// readXLMetadata - read xl metadata.
func (xl xlObjects) readXLMetadata(bucket, object string) (xlMeta xlMetaV1, err error) {
	// Count for errors encountered.
	var xlJSONErrCount = 0

	// Return the first success entry based on the selected random disk.
	for xlJSONErrCount < len(xl.storageDisks) {
		var r io.ReadCloser
		// Choose a random disk on each attempt, do not hit the same disk all the time.
		disk := xl.getRandomDisk() // Pick a random disk.
		r, err = disk.ReadFile(bucket, path.Join(object, xlMetaJSONFile), int64(0))
		if err == nil {
			defer r.Close()
			_, err = xlMeta.ReadFrom(r)
			if err == nil {
				return xlMeta, nil
			}
		}
		xlJSONErrCount++ // Update error count.
	}
	return xlMetaV1{}, err
}

// getDiskDistribution - get disk distribution.
func (xl xlObjects) getDiskDistribution() []int {
	var distribution = make([]int, len(xl.storageDisks))
	for index := range xl.storageDisks {
		distribution[index] = index + 1
	}
	return distribution
}

// writeXLJson - write `xl.json` on all disks in order.
func (xl xlObjects) writeXLMetadata(bucket, prefix string, xlMeta xlMetaV1) error {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(xl.storageDisks))

	// Initialize metadata map, save all erasure related metadata.
	xlMeta.Version = "1"
	xlMeta.Format = "xl"
	xlMeta.Minio.Release = minioReleaseTag
	xlMeta.Erasure.Algorithm = erasureAlgorithmKlauspost
	xlMeta.Erasure.DataBlocks = xl.dataBlocks
	xlMeta.Erasure.ParityBlocks = xl.parityBlocks
	xlMeta.Erasure.BlockSize = erasureBlockSize
	xlMeta.Erasure.Distribution = xl.getDiskDistribution()

	for index, disk := range xl.storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI, metadata xlMetaV1) {
			defer wg.Done()

			metaJSONFile := path.Join(prefix, xlMetaJSONFile)
			metaWriter, mErr := disk.CreateFile(bucket, metaJSONFile)
			if mErr != nil {
				mErrs[index] = mErr
				return
			}

			// Save the order.
			metadata.Erasure.Index = index + 1
			_, mErr = metadata.WriteTo(metaWriter)
			if mErr != nil {
				if mErr = safeCloseAndRemove(metaWriter); mErr != nil {
					mErrs[index] = mErr
					return
				}
				mErrs[index] = mErr
				return
			}
			if mErr = metaWriter.Close(); mErr != nil {
				if mErr = safeCloseAndRemove(metaWriter); mErr != nil {
					mErrs[index] = mErr
					return
				}
				mErrs[index] = mErr
				return
			}
			mErrs[index] = nil
		}(index, disk, xlMeta)
	}

	// Wait for all the routines.
	wg.Wait()

	// FIXME: check for quorum.
	// Return the first error.
	for _, err := range mErrs {
		if err == nil {
			continue
		}
		return err
	}
	return nil
}
