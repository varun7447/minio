package cmd

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/minio/minio/pkg/disk"

	"encoding/json"
)

const (
	// Format file name
	diskCacheFormatFile = "format.json"
	// Will be written to format.json
	diskCacheBackendVersion = "1"
	// Cached object's metadata version
	diskCacheObjectMetaVersion = "1.0.0"
	// Disk Cache format type
	diskCacheFormatType = "cachefs"
	// BoldDB bucket
	diskCacheBoltdbBucket = "cache"
	// Temporary directory where object is written to before moving
	// to the data directory.
	diskCacheTmpDir = "tmp"
	// Once the object is written to "tmp" it is committed to "data"
	diskCacheDataDir = "data"
	// BoltDB file
	diskCacheBoltDB = "meta.db"
	// prefix for locking. Preifx is used so that it does not clash with locking
	// Done by PutObjectHandler and PostPolicyBucketHandler. Prefix usage can
	// be removed once we move NS locking out of S3 layer.
	diskCacheLockingPrefix = "minio/cache/"
)

// Entry in BoltDB. Right now it stores only Atime.
type diskCacheBoltdbEntry map[string]string

// Set Atime.
func (entry diskCacheBoltdbEntry) setAtime(t time.Time) {
	entry["atime"] = t.Format(time.RFC3339)
}

// Return Atime.
func (entry diskCacheBoltdbEntry) getAtime() (time.Time, error) {
	return time.Parse(time.RFC3339, entry["atime"])
}

// Metadata of a cached object.
type diskCacheObjectMeta struct {
	// Version of the metadata structure
	Version string `json:"version"`
	// Name of the bucket of the cached object
	BucketName string `json:"bucketName"`
	// Name of the cached object
	ObjectName string `json:"objectName"`
	// Used for "Last-Modified" header
	ModTime time.Time `json:"modTime"`
	// Used for Content-Length
	Size int64 `json:"size"`
	// True if the object was cached on an anonymous request
	// Used when the backend is down and we have to know if
	// we can serve the current anonymous request
	Anonymous bool `json:"anonymous"`
	// Other user defined metadata
	HTTPMeta map[string]string `json:"httpMeta"`
}

// Converts diskCacheObjectMeta to ObjectInfo.
func (objectMeta diskCacheObjectMeta) toObjectInfo() ObjectInfo {
	objInfo := ObjectInfo{}
	objInfo.UserDefined = make(map[string]string)
	objInfo.Bucket = objectMeta.BucketName
	objInfo.Name = objectMeta.ObjectName

	objInfo.ModTime = objectMeta.ModTime
	objInfo.Size = objectMeta.Size
	objInfo.ETag = objectMeta.HTTPMeta["etag"]
	objInfo.ContentType = objectMeta.HTTPMeta["content-type"]
	objInfo.ContentEncoding = objectMeta.HTTPMeta["content-encoding"]

	for key, val := range objectMeta.HTTPMeta {
		if key == "etag" {
			continue
		}
		objInfo.UserDefined[key] = val
	}

	return objInfo
}

// Converts objInfo to diskCacheObjectMeta.
func newDiskCacheObjectMeta(objInfo ObjectInfo, anon bool) diskCacheObjectMeta {
	objMeta := diskCacheObjectMeta{}
	objMeta.HTTPMeta = make(map[string]string)
	objMeta.BucketName = objInfo.Bucket
	objMeta.ObjectName = objInfo.Name
	objMeta.ModTime = objInfo.ModTime
	objMeta.Size = objInfo.Size

	objMeta.HTTPMeta["etag"] = objInfo.ETag
	objMeta.HTTPMeta["content-type"] = objInfo.ContentType
	objMeta.HTTPMeta["content-encoding"] = objInfo.ContentEncoding

	for key, val := range objInfo.UserDefined {
		objMeta.HTTPMeta[key] = val
	}

	objMeta.Anonymous = anon
	objMeta.Version = diskCacheObjectMetaVersion
	return objMeta
}

// Disk caching
type diskCache struct {
	dir string // caching directory (--cache-dir)
	// Max disk usage limit (--cache-max) in percent. If the disk usage crosses this
	// limit then we stop caching.
	maxUsage int
	// Cache revalidation (in days)
	expiry int
	// for storing temporary files
	tmpDir string
	// for storing cached objects (renamed from tmp location)
	dataDir string
	// creation-time of the cache
	createTime time.Time
	// BoltDB for storing atime
	db *bolt.DB

	// purge() listens on this channel to start the cache-purge process
	purgeChan chan struct{}
}

// Channel entry generated by diskCache.getReadDirCh()
type diskCacheReadDirInfo struct {
	entry string
	err   error
}

// Returns if the disk usage is low.
// Disk usage is low if usage is < 80% of maxUsage
// Ex. for a 100GB disk, if maxUsage is configured as 70% then maxUsage is 70G
// hence disk usage is low if the disk usage is less than 56G (because 80% of 70G is 56G)
func (c diskCache) diskUsageLow() bool {
	minUsage := c.maxUsage * 80 / 100

	di, err := disk.GetInfo(c.dir)
	if err != nil {
		errorIf(err, "Error getting disk information on %s", c.dir)
		return false
	}
	usedPercent := (di.Total - di.Free) * 100 / di.Total
	return int(usedPercent) < minUsage
}

// Retusn if the disk usage is high.
// Disk usage is high if disk used is > maxUsage
func (c diskCache) diskUsageHigh() bool {
	di, err := disk.GetInfo(c.dir)
	if err != nil {
		return true
	}
	usedPercent := (di.Total - di.Free) * 100 / di.Total
	return int(usedPercent) > c.maxUsage
}

// Purge cache entries that were not accessed.
func (c diskCache) purge() {
	expiry := c.createTime

	for {
		for {
			if c.diskUsageLow() {
				break
			}
			d := time.Now().UTC().Sub(expiry)
			d = d / 2
			expiry = expiry.Add(d)

			c.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(diskCacheBoltdbBucket))
				cursor := b.Cursor()
				for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
					entry := make(diskCacheBoltdbEntry)
					err := json.Unmarshal(v, &entry)
					if err != nil {
						errorIf(err, "Unable to unmarshall")
						continue
					}
					atime, err := entry.getAtime()
					if err != nil {
						errorIf(err, "Unable to get atime for %s", string(k))
					}
					if atime.After(expiry) {
						continue
					}
					s := strings.SplitN(string(k), slashSeparator, 2)
					bucket := s[0]
					object := s[1]
					if bucket == "" {
						errorIf(errUnexpected, `"bucket" is empty`)
					}
					if object == "" {
						errorIf(errUnexpected, `"object" is empty`)
					}
					if err := os.Remove(c.encodedPath(bucket, object)); err != nil {
						errorIf(err, "Unable to remove %s", string(k))
						continue
					}
					if err := os.Remove(c.encodedMetaPath(bucket, object)); err != nil {
						errorIf(err, "Unable to remove meta file for %s", string(k))
						continue
					}
					err = b.Delete(k)
					errorIf(err, "Unable to delete %s", string(k))
				}
				return nil
			})
		}
		<-c.purgeChan
	}
}

// encode (bucket,object) to a hash value which is used as the file name of the cached object.
func (c diskCache) encodedPath(bucket, object string) string {
	return path.Join(c.dataDir, fmt.Sprintf("%x", sha256.Sum256([]byte(path.Join(bucket, object)))))
}

// encode (bucket,object) to a hash value which is used as the file name of the cached object's metadata.
func (c diskCache) encodedMetaPath(bucket, object string) string {
	return path.Join(c.dataDir, fmt.Sprintf("%x.json", sha256.Sum256([]byte(path.Join(bucket, object)))))
}

// Commit the cached object - rename from tmp directory to data directory.
func (c diskCache) Commit(f *os.File, objInfo ObjectInfo, anon bool) error {
	objectLock := globalNSMutex.NewNSLock(diskCacheLockingPrefix+bucket, object)
	objectLock.Lock()
	defer objectLock.Unlock()

	encPath := c.encodedPath(objInfo.Bucket, objInfo.Name)
	if err := os.Rename(f.Name(), encPath); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	metaPath := c.encodedMetaPath(objInfo.Bucket, objInfo.Name)
	objMeta := newDiskCacheObjectMeta(objInfo, anon)
	metaBytes, err := json.Marshal(objMeta)
	if err != nil {
		return err
	}

	if err = ioutil.WriteFile(metaPath, metaBytes, 0644); err != nil {
		return err
	}
	c.UpdateAtime(objInfo.Bucket, objInfo.Name, time.Now().UTC())
	return nil
}

// Remove the cached object from the tmp directory.
func (c diskCache) NoCommit(f *os.File) error {
	tmpName := f.Name()
	if err := f.Close(); err != nil {
		return err
	}

	return os.Remove(tmpName)
}

// Creates a file in the tmp directory to which the cached object is written to.
// Once the object is written to disk the caller can call diskCache.Commit()
func (c diskCache) Put(size int64) (*os.File, error) {
	if c.diskUsageHigh() {
		select {
		case c.purgeChan <- struct{}{}:
		default:
		}
		return nil, errDiskFull
	}
	f, err := ioutil.TempFile(c.tmpDir, "")
	if err != nil {
		return nil, err
	}
	e := Fallocate(int(f.Fd()), 0, size)
	switch {
	case isSysErrNoSys(e), isSysErrOpNotSupported(e), e == nil:
		// Ignore errors when Fallocate is not supported in the current system
		err = nil
	case isSysErrNoSpace(e):
		err = errDiskFull
	default:
		err = e
	}
	if err != nil {
		tmpPath := f.Name()
		f.Close()
		os.Remove(tmpPath)
		return nil, err
	}
	return f, nil
}

// Returns the handle for the cached object
func (c diskCache) Get(bucket, object string) (*os.File, ObjectInfo, bool, error) {
	objectLock := globalNSMutex.NewNSLock(diskCacheLockingPrefix+bucket, object)
	objectLock.RLock()
	defer objectLock.RUnlock()

	metaPath := c.encodedMetaPath(bucket, object)
	objMeta := diskCacheObjectMeta{}
	metaBytes, err := ioutil.ReadFile(metaPath)
	if err != nil {
		return nil, ObjectInfo{}, false, err
	}
	if err = json.Unmarshal(metaBytes, &objMeta); err != nil {
		return nil, ObjectInfo{}, false, err
	}
	if objMeta.Version != diskCacheObjectMetaVersion {
		return nil, ObjectInfo{}, false, errors.New("format not supported")
	}
	file, err := os.Open(c.encodedPath(bucket, object))
	if err != nil {
		return nil, ObjectInfo{}, false, err
	}
	c.UpdateAtime(bucket, object, time.Now().UTC())
	return file, objMeta.toObjectInfo(), objMeta.Anonymous, nil
}

// Returns metadata of the cached object
func (c diskCache) GetObjectInfo(bucket, object string) (ObjectInfo, bool, error) {
	objectLock := globalNSMutex.NewNSLock(diskCacheLockingPrefix+bucket, object)
	objectLock.RLock()
	defer objectLock.RUnlock()

	metaPath := c.encodedMetaPath(bucket, object)
	objMeta := diskCacheObjectMeta{}
	metaBytes, err := ioutil.ReadFile(metaPath)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	if err := json.Unmarshal(metaBytes, &objMeta); err != nil {
		return ObjectInfo{}, false, err
	}

	return objMeta.toObjectInfo(), objMeta.Anonymous, nil
}

// Deletes the cached object
func (c diskCache) Delete(bucket, object string) error {
	objectLock := globalNSMutex.NewNSLock(diskCacheLockingPrefix+bucket, object)
	objectLock.Lock()
	defer objectLock.Unlock()

	if err := os.Remove(c.encodedPath(bucket, object)); err != nil {
		return err
	}
	if err := os.Remove(c.encodedMetaPath(bucket, object)); err != nil {
		return err
	}
	return nil
}

// Update atime for the cached object.
func (c diskCache) UpdateAtime(bucket, object string, t time.Time) error {
	entry := make(diskCacheBoltdbEntry)
	entry.setAtime(t)
	entryBytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(diskCacheBoltdbBucket))
		return b.Put([]byte(pathJoin(bucket, object)), entryBytes)
	})
}

// format.json structure
// format.json is put in the root of the cache directory
type diskCacheFormat struct {
	Version string    `json:"version"`
	Format  string    `json:"format"`
	Time    time.Time `json:"createTime"`
}

// Inits the cache-dir if it is not init'ed already.
// Initializing implies creation of cache-dir, data-dir, tmp-dir and format.json
func newDiskCache(dir string, maxUsage, expiry int) (*diskCache, error) {
	if err := os.MkdirAll(dir, 0766); err != nil {
		return nil, err
	}
	formatPath := path.Join(dir, diskCacheFormatFile)
	formatBytes, err := ioutil.ReadFile(formatPath)
	var format diskCacheFormat
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		format.Version = diskCacheBackendVersion
		format.Format = diskCacheFormatType
		format.Time = time.Now().UTC()
		if formatBytes, err = json.Marshal(format); err != nil {
			return nil, err
		}
		if err = ioutil.WriteFile(formatPath, formatBytes, 0644); err != nil {
			return nil, err
		}
	} else {
		if err = json.Unmarshal(formatBytes, &format); err != nil {
			return nil, err
		}
		if format.Version != diskCacheBackendVersion {
			return nil, errors.New("format not supported")
		}
		if format.Format != diskCacheFormatType {
			return nil, errors.New("format not supported : " + format.Format)
		}
	}

	tmpDir := path.Join(dir, diskCacheTmpDir)
	dataDir := path.Join(dir, diskCacheDataDir)
	if err = os.MkdirAll(tmpDir, 0766); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(dataDir, 0766); err != nil {
		return nil, err
	}
	boltdbPath := pathJoin(dir, diskCacheBoltDB)
	db, err := bolt.Open(boltdbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucket([]byte(diskCacheBoltdbBucket))
		return nil
	})
	if err != nil {
		return nil, err
	}
	cache := &diskCache{dir, maxUsage, expiry, tmpDir, dataDir, format.Time, db, make(chan struct{})}

	// Start the purging go-routine
	go cache.purge()
	return cache, nil
}

// Abstracts disk caching - used by the S3 layer
type cacheObjects struct {
	dcache *diskCache

	GetObjectFn         func(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error)
	GetObjectInfoFn     func(bucket, object string) (objInfo ObjectInfo, err error)
	AnonGetObjectFn     func(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error)
	AnonGetObjectInfoFn func(bucket, object string) (objInfo ObjectInfo, err error)
	PutObjectFn         func(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error)
	AnonPutObjectFn     func(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error)
}

// Common function for GetObject and AnonGetObject
func (c cacheObjects) getObject(bucket, object string, startOffset int64, length int64, writer io.Writer, anonReq bool) (err error) {
	GetObjectFn := c.GetObjectFn
	GetObjectInfoFn := c.GetObjectInfoFn
	GetObjectInfo := c.GetObjectInfo
	if anonReq {
		GetObjectFn = c.AnonGetObjectFn
		GetObjectInfoFn = c.AnonGetObjectInfoFn
		GetObjectInfo = c.AnonGetObjectInfo
	}
	objInfo, err := GetObjectInfo(bucket, object)
	_, backendDown := errorCause(err).(BackendDown)
	if err != nil && !backendDown {
		// Do not delete cache entry
		return err
	}
	r, cachedObjInfo, anon, err := c.dcache.Get(bucket, object)
	if err == nil {
		defer r.Close()
		if backendDown {
			if anonReq {
				if anon {
					_, err = io.Copy(writer, io.NewSectionReader(r, startOffset, length))
					return err
				}
				return BackendDown{}
			}
			// If the backend is down, serve the request from cache.
			_, err = io.Copy(writer, io.NewSectionReader(r, startOffset, length))
			return err
		}
		if cachedObjInfo.ETag == objInfo.ETag {
			_, err = io.Copy(writer, io.NewSectionReader(r, startOffset, length))
			return err
		}
		c.dcache.Delete(bucket, object)
	}
	if startOffset != 0 || length != objInfo.Size {
		return GetObjectFn(bucket, object, startOffset, length, writer)
	}
	cachedObj, err := c.dcache.Put(length)
	if err == errDiskFull {
		return GetObjectFn(bucket, object, 0, objInfo.Size, writer)
	}
	if err != nil {
		return err
	}
	err = GetObjectFn(bucket, object, 0, objInfo.Size, io.MultiWriter(writer, cachedObj))
	if err != nil {
		c.dcache.NoCommit(cachedObj)
		return err
	}
	objInfo, err = GetObjectInfoFn(bucket, object)
	if err != nil {
		c.dcache.NoCommit(cachedObj)
		return err
	}
	// FIXME: race-condition: what if the server object got replaced.
	return c.dcache.Commit(cachedObj, objInfo, true)
}

// Uses cached-object to serve the request. If object is not cached it serves the request from the backend and also
// stores it in the cache for serving subsequent requests.
func (c cacheObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return c.getObject(bucket, object, startOffset, length, writer, false)
}

// Anonymous version of cacheObjects.GetObject
func (c cacheObjects) AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return c.getObject(bucket, object, startOffset, length, writer, true)
}

// Common function for GetObjectInfo and AnonGetObjectInfo
func (c cacheObjects) getObjectInfo(bucket, object string, anonReq bool) (ObjectInfo, error) {
	getObjectInfoFn := c.GetObjectInfoFn
	if anonReq {
		getObjectInfoFn = c.AnonGetObjectInfoFn
	}
	objInfo, err := getObjectInfoFn(bucket, object)
	if err != nil {
		if _, ok := errorCause(err).(BackendDown); !ok {
			return ObjectInfo{}, err
		}
		cachedObjInfo, anon, infoErr := c.dcache.GetObjectInfo(bucket, object)
		if infoErr == nil {
			if !anonReq {
				return cachedObjInfo, nil
			}
			if anon {
				return cachedObjInfo, nil
			}
		}
		return ObjectInfo{}, BackendDown{}
	}
	cachedObjInfo, _, err := c.dcache.GetObjectInfo(bucket, object)
	if err != nil {
		return objInfo, nil
	}
	if cachedObjInfo.ETag != objInfo.ETag {
		c.dcache.Delete(bucket, object)
	}
	return objInfo, nil
}

// Returns ObjectInfo from cache or the backend.
func (c cacheObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	return c.getObjectInfo(bucket, object, false)
}

// Anonymous version of cacheObjects.GetObjectInfo
func (c cacheObjects) AnonGetObjectInfo(bucket, object string) (ObjectInfo, error) {
	return c.getObjectInfo(bucket, object, true)
}

// Common code for PutObject and PutObjectInfo
func (c cacheObjects) putObject(bucket, object string, size int64, r io.Reader, metadata map[string]string, sha256sum string, anonReq bool) (ObjectInfo, error) {
	putObjectFn := c.PutObjectFn
	if anonReq {
		putObjectFn = c.AnonPutObjectFn
	}

	cachedObj, err := c.dcache.Put(size)
	if err == errDiskFull {
		return putObjectFn(bucket, object, size, r, metadata, sha256sum)
	}
	if err != nil {
		return ObjectInfo{}, err
	}
	objInfo, err := c.PutObjectFn(bucket, object, size, io.TeeReader(r, cachedObj), metadata, sha256sum)
	if err != nil {
		errNoCommit := c.dcache.NoCommit(cachedObj)
		errorIf(errNoCommit, "Error while discarding temporary cache file for %s/%s", bucket, object)
		return ObjectInfo{}, err
	}

	objInfo.Bucket = bucket
	objInfo.Name = object
	err = c.dcache.Commit(cachedObj, objInfo, anonReq)
	if err != nil {
		return ObjectInfo{}, err
	}
	return objInfo, nil
}

// PutObject - caches the uploaded object.
func (c cacheObjects) PutObject(bucket, object string, size int64, r io.Reader, metadata map[string]string, sha256sum string) (ObjectInfo, error) {
	return c.putObject(bucket, object, size, r, metadata, sha256sum, false)
}

// AnonPutObject - anonymous version of PutObject.
func (c cacheObjects) AnonPutObject(bucket, object string, size int64, r io.Reader, metadata map[string]string, sha256sum string) (ObjectInfo, error) {
	return c.putObject(bucket, object, size, r, metadata, sha256sum, true)
}

// Returns cachedObjects for use by Server.
func newServerCacheObjects(l ObjectLayer, dir string, maxUsage, expiry int) (*cacheObjects, error) {
	dcache, err := newDiskCache(dir, maxUsage, expiry)
	if err != nil {
		return nil, err
	}

	return &cacheObjects{
		dcache:          dcache,
		GetObjectFn:     l.GetObject,
		GetObjectInfoFn: l.GetObjectInfo,
		AnonGetObjectFn: func(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
			return NotImplemented{}
		},
		AnonGetObjectInfoFn: func(bucket, object string) (ObjectInfo, error) {
			return ObjectInfo{}, NotImplemented{}
		},
	}, nil
}

// Returns cachedObjects for use by Gateway.
func newGatewayCacheObjects(l GatewayLayer, dir string, maxUsage, expiry int) (*cacheObjects, error) {
	dcache, err := newDiskCache(dir, maxUsage, expiry)
	if err != nil {
		return nil, err
	}

	return &cacheObjects{
		dcache:              dcache,
		GetObjectFn:         l.GetObject,
		GetObjectInfoFn:     l.GetObjectInfo,
		PutObjectFn:         l.PutObject,
		AnonGetObjectFn:     l.AnonGetObject,
		AnonGetObjectInfoFn: l.AnonGetObjectInfo,
		AnonPutObjectFn:     l.AnonPutObject,
	}, nil
}
