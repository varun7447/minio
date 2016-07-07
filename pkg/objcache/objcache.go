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
 *
 */

// Package objcache implements in memory caching methods.
package objcache

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"sync"
	"time"
)

// NoExpiration represents caches to be permanent and can only be deleted.
var NoExpiration = time.Duration(0)

// DefaultExpiration represents default time duration value when individual
// entries will be expired.
var DefaultExpiration = time.Duration(72 * time.Hour) // 72hrs.

// buffer represents the in memory cache of a single entry.
// buffer carries value of the data and last modified time.
type buffer struct {
	value        []byte
	lastAccessed time.Time
}

// isExpired - returns true if buffer has expired.
func (b buffer) isExpired(expiration time.Duration) bool {
	return time.Now().UTC().Sub(b.lastAccessed) > expiration
}

// Cache holds the required variables to compose an in memory cache system
// which also provides expiring key mechanism and also maxSize.
type Cache struct {
	*cache
}

// why this? explained in New().
type cache struct {
	// Mutex is used for handling the concurrent
	// read/write requests for cache
	mutex sync.Mutex

	// maxSize is a total size for overall cache
	maxSize uint64

	// currentSize is a current size in memory
	currentSize uint64

	// OnEviction - callback function for eviction
	OnEviction func(key string)

	// totalEvicted counter to keep track of total expirations
	totalEvicted int

	// map of objectName and its contents
	entries map[string]*buffer

	// Expiration in time duration.
	expiration time.Duration

	// Janitor maintains carries cleanup interval
	// and stop channel for cleaning up janitor routine.
	janitor *janitor
}

// New - Return a new cache with a given default expiration duration and cleanup
// interval. If the expiration duration is less than one (or NoExpiration),
// the items in the cache never expire (by default), and must be deleted
// manually.
func New(maxSize uint64, expiration time.Duration) *Cache {
	c := &cache{
		maxSize:    maxSize,
		entries:    make(map[string]*buffer),
		expiration: expiration,
	}
	// This ensures that the janitor goroutine (which--granted it
	// was enabled--is running DeleteExpired on c forever) does not keep
	// the returned C object from being garbage collected. When it is
	// garbage collected, the finalizer stops the janitor goroutine, after
	// which c can be collected.
	C := &Cache{c}
	// We have expiration start.
	if expiration > 0 {
		// Start janitor.
		runJanitor(c, expiration)

		// The finalizer stops the janitor goroutine, after which c can be collected.
		runtime.SetFinalizer(C, stopJanitor)
	}
	return C
}

// ErrKeyNotFoundInCache - key not found in cache.
var ErrKeyNotFoundInCache = errors.New("Key not found in cache")

// ErrCacheFull - cache is full.
var ErrCacheFull = errors.New("Not enough space in cache")

// Used for adding entry to the object cache. Implements io.WriteCloser
type cacheBuffer struct {
	*bytes.Buffer // Implements io.Writer
	onClose       func() error
}

// On close, onClose() is called which checks if all object contents
// have been written so that it can save the buffer to the cache.
func (c cacheBuffer) Close() (err error) {
	return c.onClose()
}

// Create - validates if object size fits with in cache size limit and returns a io.WriteCloser
// to which object contents can be written and finally Close()'d. During Close() we
// checks if the amount of data written is equal to the size of the object, in which
// case it saves the contents to object cache.
func (c *cache) Create(key string, size int64) (w io.WriteCloser, err error) {
	// Recovers any panic generated and return errors appropriately.
	defer func() {
		if r := recover(); r != nil {
			// Recover any panic and return ErrCacheFull.
			err = ErrCacheFull
		}
	}() // Do not crash the server.

	valueLen := uint64(size)
	if c.maxSize > 0 {
		// Check if the size of the object is not bigger than the capacity of the cache.
		if valueLen > c.maxSize {
			return nil, ErrCacheFull
		}
	}

	// Will hold the object contents.
	buf := bytes.NewBuffer(make([]byte, 0, size))

	// Function called on close which saves the object contents
	// to the object cache.
	onClose := func() error {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if size != int64(buf.Len()) {
			// Full object not available hence do not save buf to object cache.
			return io.ErrShortBuffer
		}
		if c.currentSize+valueLen > c.maxSize {
			return ErrCacheFull
		}
		// Full object available in buf, save it to cache.
		c.entries[key] = &buffer{
			value:        buf.Bytes(),
			lastAccessed: time.Now().UTC(), // Save last accessed time.
		}
		// Account for the memory allocated above.
		c.currentSize += uint64(size)
		return nil
	}

	// Object contents that is written - cacheBuffer.Write(data)
	// will be accumulated in buf which implements io.Writer.
	return cacheBuffer{
		buf,
		onClose,
	}, nil
}

// Open - open the in-memory file, returns an in memory read seeker.
// returns an error ErrNotFoundInCache, if the key does not exist.
func (c *cache) Open(key string) (io.ReadSeeker, error) {
	// Entry exists, return the readable buffer.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	buf, ok := c.entries[key]
	if !ok {
		return nil, ErrKeyNotFoundInCache
	}
	buf.lastAccessed = time.Now().UTC()
	return bytes.NewReader(buf.value), nil
}

// Delete - delete deletes an entry from the cache.
func (c *cache) Delete(key string) {
	c.mutex.Lock()
	c.delete(key)
	c.mutex.Unlock()
	if c.OnEviction != nil {
		c.OnEviction(key)
	}
}

// DeleteExpired - deletes all the expired entries from the cache.
func (c *cache) DeleteExpired() {
	var evictedEntries []string
	c.mutex.Lock()
	for k, v := range c.entries {
		if v.isExpired(c.expiration) {
			c.delete(k)
			evictedEntries = append(evictedEntries, k)
		}
	}
	c.mutex.Unlock()
	for _, k := range evictedEntries {
		if c.OnEviction != nil {
			c.OnEviction(k)
		}
	}
}

// Deletes a requested entry from the cache.
func (c *cache) delete(key string) {
	if buf, ok := c.entries[key]; ok {
		delete(c.entries, key)
		c.currentSize -= uint64(len(buf.value))
		c.totalEvicted++
	}
}
