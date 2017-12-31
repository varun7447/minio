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
	"runtime/debug"
	"sort"
	"sync"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/objcache"
)

// XL constants.
const (
	// XL metadata file carries per object metadata.
	xlMetaJSONFile = "xl.json"

	// Uploads metadata file carries per multipart object metadata.
	uploadsJSONFile = "uploads.json"

	// Represents the minimum required RAM size to enable caching.
	minRAMSize = 24 * humanize.GiByte

	// Maximum erasure blocks.
	maxErasureBlocks = 16

	// Minimum erasure blocks.
	minErasureBlocks = 4
)

// xlObjects - Implements XL object layer.
type xlObjects struct {
	mutex *sync.Mutex

	// ListObjects pool management.
	listPool *treeWalkPool

	// Object cache for caching objects.
	objCache *objcache.Cache

	// Object cache enabled.
	objCacheEnabled bool

	xls          *xlSets
	storageDisks func() []StorageAPI
}

// list of all errors that can be ignored in tree walk operation in XL
var xlTreeWalkIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound)

// newXLObjects - initialize new xl object layer.
func newXLObjects(xls *xlSets, getDisks func() []StorageAPI) *xlObjects {
	// Initialize list pool.
	listPool := newTreeWalkPool(globalLookupTimeout)

	// Initialize xl objects.
	xl := &xlObjects{
		mutex:        &sync.Mutex{},
		listPool:     listPool,
		xls:          xls,
		storageDisks: getDisks,
	}

	// Get cache size if _MINIO_CACHE environment variable is set.
	var maxCacheSize uint64
	var err error
	if !globalXLObjCacheDisabled {
		maxCacheSize, err = GetMaxCacheSize()
		errorIf(err, "Unable to get maximum cache size")

		// Enable object cache if cache size is more than zero
		xl.objCacheEnabled = maxCacheSize > 0
	}

	// Check if object cache is enabled.
	if xl.objCacheEnabled {
		// Initialize object cache.
		objCache, oerr := objcache.New(maxCacheSize, objcache.DefaultExpiry)
		if oerr != nil {
			return nil
		}
		objCache.OnEviction = func(key string) {
			debug.FreeOSMemory()
		}
		xl.objCache = objCache
	}

	return xl
}

// Shutdown function for object storage interface.
func (xl xlObjects) Shutdown() error {
	// Add any object layer shutdown activities here.
	for _, disk := range xl.storageDisks() {
		// This closes storage rpc client connections if any.
		// Otherwise this is a no-op.
		if disk == nil {
			continue
		}
		disk.Close()
	}
	return nil
}

// byDiskTotal is a collection satisfying sort.Interface.
type byDiskTotal []disk.Info

func (d byDiskTotal) Len() int      { return len(d) }
func (d byDiskTotal) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d byDiskTotal) Less(i, j int) bool {
	return d[i].Total < d[j].Total
}

// getDisksInfo - fetch disks info across all other storage API.
func getDisksInfo(disks []StorageAPI) (disksInfo []disk.Info, onlineDisks int, offlineDisks int) {
	disksInfo = make([]disk.Info, len(disks))
	for i, storageDisk := range disks {
		if storageDisk == nil {
			// Storage disk is empty, perhaps ignored disk or not available.
			offlineDisks++
			continue
		}
		info, err := storageDisk.DiskInfo()
		if err != nil {
			errorIf(err, "Unable to fetch disk info for %#v", storageDisk)
			if errors.IsErr(err, baseErrs...) {
				offlineDisks++
				continue
			}
		}
		onlineDisks++
		disksInfo[i] = info
	}

	// Success.
	return disksInfo, onlineDisks, offlineDisks
}

// returns sorted disksInfo slice which has only valid entries.
// i.e the entries where the total size of the disk is not stated
// as 0Bytes, this means that the disk is not online or ignored.
func sortValidDisksInfo(disksInfo []disk.Info) []disk.Info {
	var validDisksInfo []disk.Info
	for _, diskInfo := range disksInfo {
		if diskInfo.Total == 0 {
			continue
		}
		validDisksInfo = append(validDisksInfo, diskInfo)
	}
	sort.Sort(byDiskTotal(validDisksInfo))
	return validDisksInfo
}

// Get an aggregated storage info across all disks.
func getStorageInfo(disks []StorageAPI) StorageInfo {
	disksInfo, onlineDisks, offlineDisks := getDisksInfo(disks)

	// Sort so that the first element is the smallest.
	validDisksInfo := sortValidDisksInfo(disksInfo)
	// If there are no valid disks, set total and free disks to 0
	if len(validDisksInfo) == 0 {
		return StorageInfo{
			Total: 0,
			Free:  0,
		}
	}

	// Return calculated storage info, choose the lowest Total and
	// Free as the total aggregated values. Total capacity is always
	// the multiple of smallest disk among the disk list.
	storageInfo := StorageInfo{
		Total: validDisksInfo[0].Total * uint64(onlineDisks) / 2,
		Free:  validDisksInfo[0].Free * uint64(onlineDisks) / 2,
	}

	storageInfo.Backend.Type = Erasure
	storageInfo.Backend.OnlineDisks = onlineDisks
	storageInfo.Backend.OfflineDisks = offlineDisks

	_, scParity := getRedundancyCount(standardStorageClass, len(disks))
	storageInfo.Backend.standardSCParity = scParity

	_, rrSCparity := getRedundancyCount(reducedRedundancyStorageClass, len(disks))
	storageInfo.Backend.rrSCParity = rrSCparity

	return storageInfo
}

// StorageInfo - returns underlying storage statistics.
func (xl xlObjects) StorageInfo() StorageInfo {
	storageInfo := getStorageInfo(xl.storageDisks())
	return storageInfo
}
