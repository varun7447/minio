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

package objcache

import "time"

// janitor represents janitor routine data type.
// this data type carries ticker interval and a
// stop channel to close the routine.
type janitor struct {
	expiration time.Duration
	stop       chan bool
}

// Run runs the janitor routine ticking at interval.
func (j *janitor) run(c *cache) {
	j.stop = make(chan bool)
	ticker := time.NewTicker(j.expiration)
	for {
		select {
		// For each tick run delete expired entries.
		case <-ticker.C:
			c.DeleteExpired()
		// Stop the routine.
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}

// stopJanitor - stop janitor go-routine, runtime finalizer will invoke this
// stopping the routine while it garbage collects.
func stopJanitor(c *Cache) {
	c.janitor.stop <- true
}

// runJanitor - run janitor for the input cache runs at input cleanup interval.
func runJanitor(c *cache, expiration time.Duration) {
	j := &janitor{
		expiration: expiration,
	}
	c.janitor = j
	// Stars the janitor routine.
	go j.run(c)
}
