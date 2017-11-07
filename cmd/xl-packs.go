/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"io"
	"sort"
	"strings"

	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
	"golang.org/x/sync/errgroup"
)

// xlPacks implements Objectlayer combining a static list of erasure coded
// object layers. NOTE: There is no dynamic scaling allowed or intended in
// current design.
type xlPacks struct {
	layers []*xlObjects

	// Pack level listObjects pool management.
	listPool *treeWalkPool
}

// Initialize new set of erasure coded packs.
func newXLPacks(layers []*xlObjects) ObjectLayer {
	return &xlPacks{
		layers:   layers,
		listPool: newTreeWalkPool(globalLookupTimeout),
	}
}

// StorageInfo - combines output of StorageInfo across all erasure
// coded object layers.
func (s xlPacks) StorageInfo() StorageInfo {
	var info StorageInfo
	info.Backend.Type = Erasure
	for _, layer := range s.layers {
		linfo := layer.StorageInfo()
		info.Total = info.Total + linfo.Total
		info.Free = info.Free + linfo.Free
		info.Backend.OnlineDisks = info.Backend.OnlineDisks + linfo.Backend.OnlineDisks
		info.Backend.OfflineDisks = info.Backend.OfflineDisks + linfo.Backend.OfflineDisks
	}
	return info
}

func (s xlPacks) Shutdown() error {
	var g errgroup.Group

	for _, layer := range s.layers {
		layer := layer // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return layer.Shutdown()
		})
	}

	return g.Wait()
}

func (s xlPacks) MakeBucketWithLocation(bucket, location string) error {
	var g errgroup.Group

	for _, layer := range s.layers {
		layer := layer // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return layer.MakeBucketWithLocation(bucket, location)
		})
	}

	// Wait for all makebucket to finish.
	return g.Wait()
}

func (s xlPacks) getLoadBalancedLayers(input string) (layers []*xlObjects) {
	// Based on the random shuffling return back randomized disks.
	for _, i := range hashOrder(input, len(s.layers)) {
		layers = append(layers, s.layers[i-1])
	}
	return layers
}

func (s xlPacks) getHashedLayer(input string) (layers *xlObjects) {
	return s.getLoadBalancedLayers(input)[0]
}

func (s xlPacks) GetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	for _, layer := range s.getLoadBalancedLayers(UTCNow().String()) {
		bucketInfo, err = layer.GetBucketInfo(bucket)
		if err != nil {
			return bucketInfo, err
		}
		break
	}
	return bucketInfo, nil
}

func (s xlPacks) DeleteBucket(bucket string) error {
	var g errgroup.Group

	for _, layer := range s.layers {
		layer := layer // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return layer.DeleteBucket(bucket)
		})
	}

	return g.Wait()
}

func (s xlPacks) ListBuckets() (buckets []BucketInfo, err error) {
	for _, layer := range s.getLoadBalancedLayers(UTCNow().String()) {
		buckets, err = layer.ListBuckets()
		if err != nil {
			return nil, err
		}
		break
	}
	return buckets, nil
}

func (s xlPacks) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
	return s.getHashedLayer(object).GetObject(bucket, object, startOffset, length, writer)
}

func (s xlPacks) PutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	return s.getHashedLayer(object).PutObject(bucket, object, data, metadata)
}

func (s xlPacks) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return s.getHashedLayer(object).GetObjectInfo(bucket, object)
}

func (s xlPacks) DeleteObject(bucket string, object string) (err error) {
	return s.getHashedLayer(object).DeleteObject(bucket, object)
}

func (s xlPacks) CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	srcLayer := s.getHashedLayer(srcObject)
	destLayer := s.getHashedLayer(destObject)

	objInfo, err = srcLayer.GetObjectInfo(srcBucket, srcObject)
	if err != nil {
		return objInfo, err
	}

	// Check if this request is only metadata update.
	cpMetadataOnly := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(destBucket, destObject))
	if cpMetadataOnly {
		return srcLayer.CopyObject(srcBucket, srcObject, destBucket, destObject, metadata)
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		if gerr := srcLayer.GetObject(srcBucket, srcObject, 0, objInfo.Size, pipeWriter); gerr != nil {
			errorIf(gerr, "Unable to read %s of the object `%s/%s`.", srcBucket, srcObject)
			pipeWriter.CloseWithError(toObjectErr(gerr, srcBucket, srcObject))
			return
		}
		pipeWriter.Close() // Close writer explicitly signalling we wrote all data.
	}()

	hashReader, err := hash.NewReader(pipeReader, objInfo.Size, "", "")
	if err != nil {
		pipeReader.CloseWithError(err)
		return objInfo, toObjectErr(errors.Trace(err), destBucket, destObject)
	}

	objInfo, err = destLayer.PutObject(destBucket, destObject, hashReader, metadata)
	if err != nil {
		pipeReader.CloseWithError(err)
		return objInfo, err
	}

	// Explicitly close the reader.
	pipeReader.Close()

	return objInfo, nil
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry is a leaf or non-leaf entry.
// disks - used for doing disk.ListDir(). FS passes single disk argument, XL passes a list of disks.
func listDirPacksFactory(isLeaf isLeafFunc, treeWalkIgnoredErrs []error, packs ...[]StorageAPI) listDirFunc {
	listDirInternal := func(bucket, prefixDir, prefixEntry string, disks []StorageAPI) (entries []string, err error) {
		for _, disk := range disks {
			if disk == nil {
				continue
			}
			entries, err = disk.ListDir(bucket, prefixDir)
			if err != nil {
				// For any reason disk was deleted or goes offline, continue
				// and list from other disks if possible.
				if errors.IsErrIgnored(err, treeWalkIgnoredErrs...) {
					continue
				}
				return nil, errors.Trace(err)
			}

			// Filter entries that have the prefix prefixEntry.
			entries = filterMatchingPrefix(entries, prefixEntry)

			// isLeaf() check has to happen here so that
			// trailing "/" for objects can be removed.
			for i, entry := range entries {
				if isLeaf(bucket, pathJoin(prefixDir, entry)) {
					entries[i] = strings.TrimSuffix(entry, slashSeparator)
				}
			}
			break
		}
		return entries, nil
	}

	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (mergedEntries []string, delayIsLeaf bool, err error) {
		for _, disks := range packs {
			var entries []string
			entries, err = listDirInternal(bucket, prefixDir, prefixEntry, disks)
			if err != nil {
				return nil, false, err
			}

			var newEntries []string
			// Find elements in entries which are not in mergedEntries
			for _, entry := range entries {
				idx := sort.SearchStrings(mergedEntries, entry)
				// if entry is already present in mergedEntries don't add.
				if idx < len(mergedEntries) && mergedEntries[idx] == entry {
					continue
				}
				newEntries = append(newEntries, entry)
			}

			if len(newEntries) > 0 {
				// Merge the entries and sort it.
				mergedEntries = append(mergedEntries, newEntries...)
				sort.Strings(mergedEntries)
			}
		}

		return mergedEntries, false, nil
	}
	return listDir
}

func (s *xlPacks) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	var objInfos []ObjectInfo
	var eof bool
	var nextMarker string

	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	walkResultCh, endWalkCh := s.listPool.Release(listParams{bucket, recursive, marker, prefix, false})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		isLeaf := func(bucket, entry string) bool {
			entry = strings.TrimSuffix(entry, slashSeparator)
			return s.getHashedLayer(entry).isObject(bucket, entry)
		}

		var packDisks = make([][]StorageAPI, len(s.layers))
		for _, layer := range s.layers {
			packDisks = append(packDisks, layer.getLoadBalancedDisks())
		}
		listDir := listDirPacksFactory(isLeaf, xlTreeWalkIgnoredErrs, packDisks...)
		walkResultCh = startTreeWalk(bucket, prefix, marker, recursive, listDir, isLeaf, endWalkCh)
	}

	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			return result, toObjectErr(walkResult.err, bucket, prefix)
		}

		entry := walkResult.entry
		var objInfo ObjectInfo
		if hasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			objInfo.Bucket = bucket
			objInfo.Name = entry
			objInfo.IsDir = true
		} else {
			// Set the Mode to a "regular" file.
			var err error
			objInfo, err = s.getHashedLayer(entry).getObjectInfo(bucket, entry)
			if err != nil {
				// Ignore errFileNotFound
				if errors.Cause(err) == errFileNotFound {
					continue
				}
				return result, toObjectErr(err, bucket, prefix)
			}
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		i++
		if walkResult.end {
			eof = true
			break
		}
	}

	params := listParams{bucket, recursive, nextMarker, prefix, false}
	if !eof {
		s.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result = ListObjectsInfo{IsTruncated: !eof}
	for _, objInfo := range objInfos {
		result.NextMarker = objInfo.Name
		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}
	return result, nil
}

func (s xlPacks) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// In list multipart uploads treating the prefix as the object itself and not going to perform any merge sort operations.
	// This is similar in implementation to fs backend.
	return s.getHashedLayer(prefix).ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

func (s xlPacks) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	return s.getHashedLayer(object).NewMultipartUpload(bucket, object, metadata)
}

func (s xlPacks) CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
	startOffset int64, length int64, metadata map[string]string) (partInfo PartInfo, err error) {

	srcLayer := s.getHashedLayer(srcObject)
	destLayer := s.getHashedLayer(destObject)

	// Initialize pipe to stream from source.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		if gerr := srcLayer.GetObject(srcBucket, srcObject, startOffset, length, pipeWriter); gerr != nil {
			errorIf(gerr, "Unable to read %s of the object `%s/%s`.", srcBucket, srcObject)
			pipeWriter.CloseWithError(toObjectErr(gerr, srcBucket, srcObject))
			return
		}
		// Close writer explicitly signalling we wrote all data.
		pipeWriter.Close()
		return
	}()

	hashReader, err := hash.NewReader(pipeReader, length, "", "")
	if err != nil {
		pipeReader.CloseWithError(err)
		return partInfo, toObjectErr(errors.Trace(err), destBucket, destObject)
	}

	partInfo, err = destLayer.PutObjectPart(destBucket, destObject, uploadID, partID, hashReader)
	if err != nil {
		pipeReader.CloseWithError(err)
		return partInfo, err
	}

	// Close the pipe
	pipeReader.Close()

	return partInfo, nil
}

func (s xlPacks) PutObjectPart(bucket, object, uploadID string, partID int, data *hash.Reader) (info PartInfo, err error) {
	return s.getHashedLayer(object).PutObjectPart(bucket, object, uploadID, partID, data)
}

func (s xlPacks) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	return s.getHashedLayer(object).ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
}

func (s xlPacks) AbortMultipartUpload(bucket, object, uploadID string) error {
	return s.getHashedLayer(object).AbortMultipartUpload(bucket, object, uploadID)
}

func (s xlPacks) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []CompletePart) (objInfo ObjectInfo, err error) {
	return s.getHashedLayer(object).CompleteMultipartUpload(bucket, object, uploadID, uploadedParts)
}

func (s xlPacks) HealBucket(bucket string) (err error) {
	var g errgroup.Group

	for _, layer := range s.layers {
		layer := layer // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return layer.HealBucket(bucket)
		})
	}

	return g.Wait()
}

func (s xlPacks) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return buckets, errors.Trace(NotImplemented{})
}

func (s xlPacks) HealObject(bucket, object string) (int, int, error) {
	return s.getHashedLayer(object).HealObject(bucket, object)
}

func (s xlPacks) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, errors.Trace(NotImplemented{})
}

func (s xlPacks) ListUploadsHeal(bucket, prefix, marker, uploadIDMarker,
	delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return ListMultipartsInfo{}, errors.Trace(NotImplemented{})
}
