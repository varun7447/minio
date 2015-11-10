/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-xl/pkg/probe"
)

func (fs Filesystem) listWorker(startReq ListObjectsReq) (chan<- listWorkerReq, *probe.Error) {
	Separator := string(os.PathSeparator)
	bucket := startReq.Bucket
	prefix := startReq.Prefix
	marker := startReq.Marker
	delimiter := startReq.Delimiter
	quit := make(chan bool)

	fmt.Println("listWorker being started:", bucket, prefix, marker, delimiter)
	if marker != "" {
		return nil, probe.NewError(errors.New("Not supported"))
	}
	if delimiter != "" && delimiter != Separator {
		return nil, probe.NewError(errors.New("Not supported"))
	}
	reqCh := make(chan listWorkerReq)
	walkerCh := make(chan ObjectMetadata)
	go func() {
		rootPath := filepath.Join(fs.path, bucket, prefix)
		stripPath := filepath.Join(fs.path, bucket) + Separator
		filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
			if path == rootPath {
				return nil
			}
			if info.IsDir() {
				path = path + Separator
			}
			objectName := strings.TrimPrefix(path, stripPath)
			object := ObjectMetadata{
				Object:  objectName,
				Created: info.ModTime(),
				Mode:    info.Mode(),
				Size:    info.Size(),
			}
			select {
			case walkerCh <- object:
				// do nothings
			case <-quit:
				fmt.Println("walker got quit")
				// returning error ends the Walk()
				return errors.New("Ending")
			}
			if delimiter == Separator && info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		})
		close(walkerCh)
	}()
	go func() {
		resp := ListObjectsResp{}
		for {
			select {
			case <-time.After(10 * time.Second):
				fmt.Println("worker got timeout")
				quit <- true
				timeoutReq := ListObjectsReq{bucket, prefix, marker, delimiter, 0}
				fmt.Println("after timeout", fs)
				fs.timeoutReqCh <- timeoutReq
				// FIXME: can there be a race such that sender on reqCh panics?
				return
			case req := <-reqCh:
				resp = ListObjectsResp{}
				resp.Objects = make([]ObjectMetadata, 0)
				resp.Prefixes = make([]string, 0)
				count := 0
				for object := range walkerCh {
					if object.Mode.IsDir() {
						if delimiter == "" {
							// skip directories for recursive list
							continue
						}
						resp.Prefixes = append(resp.Prefixes, object.Object)
					} else {
						resp.Objects = append(resp.Objects, object)
					}
					resp.NextMarker = object.Object
					count++
					if count == req.req.MaxKeys {
						resp.IsTruncated = true
						break
					}
				}
				fmt.Println("response objects: ", len(resp.Objects))
				marker = resp.NextMarker
				req.respCh <- resp
			}
		}
	}()
	return reqCh, nil
}

func (fs *Filesystem) startListService() *probe.Error {
	fmt.Println("startListService starting")
	listServiceReqCh := make(chan listServiceReq)
	timeoutReqCh := make(chan ListObjectsReq)
	reqToListWorkerReqCh := make(map[string](chan<- listWorkerReq))
	reqToStr := func(bucket string, prefix string, marker string, delimiter string) string {
		return strings.Join([]string{bucket, prefix, marker, delimiter}, ":")
	}
	go func() {
		for {
			select {
			case timeoutReq := <-timeoutReqCh:
				fmt.Println("listservice got timeout on ", timeoutReq)
				reqStr := reqToStr(timeoutReq.Bucket, timeoutReq.Prefix, timeoutReq.Marker, timeoutReq.Delimiter)
				listWorkerReqCh, ok := reqToListWorkerReqCh[reqStr]
				if ok {
					close(listWorkerReqCh)
				}
				delete(reqToListWorkerReqCh, reqStr)
			case serviceReq := <-listServiceReqCh:
				fmt.Println("serviceReq received", serviceReq)
				fmt.Println("sending to listservicereqch", fs)

				reqStr := reqToStr(serviceReq.req.Bucket, serviceReq.req.Prefix, serviceReq.req.Marker, serviceReq.req.Delimiter)
				listWorkerReqCh, ok := reqToListWorkerReqCh[reqStr]
				if !ok {
					var err *probe.Error
					listWorkerReqCh, err = fs.listWorker(serviceReq.req)
					if err != nil {
						fmt.Println("listWorker returned error", err)
						serviceReq.respCh <- ListObjectsResp{}
						return
					}
					reqToListWorkerReqCh[reqStr] = listWorkerReqCh
				}
				respCh := make(chan ListObjectsResp)
				listWorkerReqCh <- listWorkerReq{serviceReq.req, respCh}
				resp, ok := <-respCh
				if !ok {
					serviceReq.respCh <- ListObjectsResp{}
					fmt.Println("listWorker resp was not ok")
					return
				}
				delete(reqToListWorkerReqCh, reqStr)
				if !resp.IsTruncated {
					close(listWorkerReqCh)
				} else {
					reqStr = reqToStr(serviceReq.req.Bucket, serviceReq.req.Prefix, resp.NextMarker, serviceReq.req.Delimiter)
					reqToListWorkerReqCh[reqStr] = listWorkerReqCh
				}
				serviceReq.respCh <- resp
			}
		}
	}()
	fs.timeoutReqCh = timeoutReqCh
	fs.listServiceReqCh = listServiceReqCh
	return nil
}

func (fs Filesystem) ListObjects(bucket string, req ListObjectsReq) (ListObjectsResp, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	Separator := string(os.PathSeparator)

	if !IsValidBucket(bucket) {
		return ListObjectsResp{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	canonicalize := func(str string) string {
		return strings.Replace(str, "/", string(os.PathSeparator), -1)
	}
	decanonicalize := func(str string) string {
		return strings.Replace(str, string(os.PathSeparator), "/", -1)
	}

	req.Bucket = bucket
	req.Prefix = canonicalize(req.Prefix)
	req.Marker = canonicalize(req.Marker)
	req.Delimiter = canonicalize(req.Delimiter)

	if req.Delimiter != "" && req.Delimiter != Separator {
		return ListObjectsResp{}, probe.NewError(errors.New("not supported"))
	}

	respCh := make(chan ListObjectsResp)
	fs.listServiceReqCh <- listServiceReq{req, respCh}
	resp := <-respCh

	for i := 0; i < len(resp.Prefixes); i++ {
		resp.Prefixes[i] = decanonicalize(resp.Prefixes[i])
	}
	for i := 0; i < len(resp.Objects); i++ {
		resp.Objects[i].Object = decanonicalize(resp.Objects[i].Object)
	}
	if req.Delimiter == "" {
		// unset NextMaker for recursive list
		resp.NextMarker = ""
	}
	return resp, nil
}

// ListObjects - GET bucket (list objects)
// func (fs Filesystem) ListObjects(bucket string, resources BucketResourcesMetadata) ([]ObjectMetadata, BucketResourcesMetadata, *probe.Error) {
// 	fs.lock.Lock()
// 	defer fs.lock.Unlock()
// 	if !IsValidBucket(bucket) {
// 		return nil, resources, probe.NewError(BucketNameInvalid{Bucket: bucket})
// 	}
// 	if resources.Prefix != "" && IsValidObjectName(resources.Prefix) == false {
// 		return nil, resources, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: resources.Prefix})
// 	}
//
// 	p := bucketDir{}
// 	rootPrefix := filepath.Join(fs.path, bucket)
// 	// check bucket exists
// 	if _, err := os.Stat(rootPrefix); os.IsNotExist(err) {
// 		return nil, resources, probe.NewError(BucketNotFound{Bucket: bucket})
// 	}
//
// 	p.root = rootPrefix
// 	/// automatically treat incoming "/" as "\\" on windows due to its path constraints.
// 	if runtime.GOOS == "windows" {
// 		if resources.Prefix != "" {
// 			resources.Prefix = strings.Replace(resources.Prefix, "/", string(os.PathSeparator), -1)
// 		}
// 		if resources.Delimiter != "" {
// 			resources.Delimiter = strings.Replace(resources.Delimiter, "/", string(os.PathSeparator), -1)
// 		}
// 		if resources.Marker != "" {
// 			resources.Marker = strings.Replace(resources.Marker, "/", string(os.PathSeparator), -1)
// 		}
// 	}
//
// 	// if delimiter is supplied and not prefix then we are the very top level, list everything and move on.
// 	if resources.Delimiter != "" && resources.Prefix == "" {
// 		files, err := ioutil.ReadDir(rootPrefix)
// 		if err != nil {
// 			if os.IsNotExist(err) {
// 				return nil, resources, probe.NewError(BucketNotFound{Bucket: bucket})
// 			}
// 			return nil, resources, probe.NewError(err)
// 		}
// 		for _, fl := range files {
// 			p.files = append(p.files, contentInfo{
// 				Prefix:   fl.Name(),
// 				Size:     fl.Size(),
// 				Mode:     fl.Mode(),
// 				ModTime:  fl.ModTime(),
// 				FileInfo: fl,
// 			})
// 		}
// 	}
//
// 	// If delimiter and prefix is supplied make sure that paging doesn't go deep, treat it as simple directory listing.
// 	if resources.Delimiter != "" && resources.Prefix != "" {
// 		if !strings.HasSuffix(resources.Prefix, resources.Delimiter) {
// 			fl, err := os.Stat(filepath.Join(rootPrefix, resources.Prefix))
// 			if err != nil {
// 				if os.IsNotExist(err) {
// 					return nil, resources, probe.NewError(ObjectNotFound{Bucket: bucket, Object: resources.Prefix})
// 				}
// 				return nil, resources, probe.NewError(err)
// 			}
// 			p.files = append(p.files, contentInfo{
// 				Prefix:   resources.Prefix,
// 				Size:     fl.Size(),
// 				Mode:     os.ModeDir,
// 				ModTime:  fl.ModTime(),
// 				FileInfo: fl,
// 			})
// 		} else {
// 			files, err := ioutil.ReadDir(filepath.Join(rootPrefix, resources.Prefix))
// 			if err != nil {
// 				if os.IsNotExist(err) {
// 					return nil, resources, probe.NewError(ObjectNotFound{Bucket: bucket, Object: resources.Prefix})
// 				}
// 				return nil, resources, probe.NewError(err)
// 			}
// 			for _, fl := range files {
// 				prefix := fl.Name()
// 				if resources.Prefix != "" {
// 					prefix = filepath.Join(resources.Prefix, fl.Name())
// 				}
// 				p.files = append(p.files, contentInfo{
// 					Prefix:   prefix,
// 					Size:     fl.Size(),
// 					Mode:     fl.Mode(),
// 					ModTime:  fl.ModTime(),
// 					FileInfo: fl,
// 				})
// 			}
// 		}
// 	}
// 	if resources.Delimiter == "" {
// 		var files []contentInfo
// 		getAllFiles := func(fp string, fl os.FileInfo, err error) error {
// 			// If any error return back quickly
// 			if err != nil {
// 				return err
// 			}
// 			if strings.HasSuffix(fp, "$multiparts") {
// 				return nil
// 			}
// 			// if file pointer equals to rootPrefix - discard it
// 			if fp == p.root {
// 				return nil
// 			}
// 			if len(files) > resources.Maxkeys {
// 				return ErrSkipFile
// 			}
// 			// Split the root prefix from the incoming file pointer
// 			realFp := ""
// 			if runtime.GOOS == "windows" {
// 				if splits := strings.Split(fp, (p.root + string(os.PathSeparator))); len(splits) > 1 {
// 					realFp = splits[1]
// 				}
// 			} else {
// 				if splits := strings.Split(fp, (p.root + string(os.PathSeparator))); len(splits) > 1 {
// 					realFp = splits[1]
// 				}
// 			}
// 			// If path is a directory and has a prefix verify if the file pointer
// 			// has the prefix if it does not skip the directory.
// 			if fl.Mode().IsDir() {
// 				if resources.Prefix != "" {
// 					// Skip the directory on following situations
// 					// - when prefix is part of file pointer along with the root path
// 					// - when file pointer is part of the prefix along with root path
// 					if !strings.HasPrefix(fp, filepath.Join(p.root, resources.Prefix)) &&
// 						!strings.HasPrefix(filepath.Join(p.root, resources.Prefix), fp) {
// 						return ErrSkipDir
// 					}
// 				}
// 			}
// 			// If path is a directory and has a marker verify if the file split file pointer
// 			// is lesser than the Marker top level directory if yes skip it.
// 			if fl.Mode().IsDir() {
// 				if resources.Marker != "" {
// 					if realFp != "" {
// 						// For windows split with its own os.PathSeparator
// 						if runtime.GOOS == "windows" {
// 							if realFp < strings.Split(resources.Marker, string(os.PathSeparator))[0] {
// 								return ErrSkipDir
// 							}
// 						} else {
// 							if realFp < strings.Split(resources.Marker, string(os.PathSeparator))[0] {
// 								return ErrSkipDir
// 							}
// 						}
// 					}
// 				}
// 			}
// 			// If regular file verify
// 			if fl.Mode().IsRegular() {
// 				// If marker is present this will be used to check if filepointer is
// 				// lexically higher than then Marker
// 				if realFp != "" {
// 					if resources.Marker != "" {
// 						if realFp > resources.Marker {
// 							files = append(files, contentInfo{
// 								Prefix:   realFp,
// 								Size:     fl.Size(),
// 								Mode:     fl.Mode(),
// 								ModTime:  fl.ModTime(),
// 								FileInfo: fl,
// 							})
// 						}
// 					} else {
// 						files = append(files, contentInfo{
// 							Prefix:   realFp,
// 							Size:     fl.Size(),
// 							Mode:     fl.Mode(),
// 							ModTime:  fl.ModTime(),
// 							FileInfo: fl,
// 						})
// 					}
// 				}
// 			}
// 			// If file is a symlink follow it and populate values.
// 			if fl.Mode()&os.ModeSymlink == os.ModeSymlink {
// 				st, err := os.Stat(fp)
// 				if err != nil {
// 					return nil
// 				}
// 				// If marker is present this will be used to check if filepointer is
// 				// lexically higher than then Marker
// 				if realFp != "" {
// 					if resources.Marker != "" {
// 						if realFp > resources.Marker {
// 							files = append(files, contentInfo{
// 								Prefix:   realFp,
// 								Size:     st.Size(),
// 								Mode:     st.Mode(),
// 								ModTime:  st.ModTime(),
// 								FileInfo: st,
// 							})
// 						}
// 					} else {
// 						files = append(files, contentInfo{
// 							Prefix:   realFp,
// 							Size:     st.Size(),
// 							Mode:     st.Mode(),
// 							ModTime:  st.ModTime(),
// 							FileInfo: st,
// 						})
// 					}
// 				}
// 			}
// 			p.files = files
// 			return nil
// 		}
// 		// If no delimiter is specified, crawl through everything.
// 		err := Walk(rootPrefix, getAllFiles)
// 		if err != nil {
// 			if os.IsNotExist(err) {
// 				return nil, resources, probe.NewError(ObjectNotFound{Bucket: bucket, Object: resources.Prefix})
// 			}
// 			return nil, resources, probe.NewError(err)
// 		}
// 	}
//
// 	var metadataList []ObjectMetadata
// 	var metadata ObjectMetadata
//
// 	// Filter objects
// 	for _, content := range p.files {
// 		if len(metadataList) == resources.Maxkeys {
// 			resources.IsTruncated = true
// 			if resources.IsTruncated && resources.Delimiter != "" {
// 				resources.NextMarker = metadataList[len(metadataList)-1].Object
// 			}
// 			break
// 		}
// 		if content.Prefix > resources.Marker {
// 			var err *probe.Error
// 			metadata, resources, err = fs.filterObjects(bucket, content, resources)
// 			if err != nil {
// 				return nil, resources, err.Trace()
// 			}
// 			// If windows replace all the incoming paths to API compatible paths
// 			if runtime.GOOS == "windows" {
// 				metadata.Object = sanitizeWindowsPath(metadata.Object)
// 			}
// 			if metadata.Bucket != "" {
// 				metadataList = append(metadataList, metadata)
// 			}
// 		}
// 	}
// 	// Sanitize common prefixes back into API compatible paths
// 	if runtime.GOOS == "windows" {
// 		resources.CommonPrefixes = sanitizeWindowsPaths(resources.CommonPrefixes...)
// 	}
// 	return metadataList, resources, nil
// }
//
// func (fs Filesystem) filterObjects(bucket string, content contentInfo, resources BucketResourcesMetadata) (ObjectMetadata, BucketResourcesMetadata, *probe.Error) {
// 	var err *probe.Error
// 	var metadata ObjectMetadata
//
// 	name := content.Prefix
// 	switch true {
// 	// Both delimiter and Prefix is present
// 	case resources.Delimiter != "" && resources.Prefix != "":
// 		if strings.HasPrefix(name, resources.Prefix) {
// 			trimmedName := strings.TrimPrefix(name, resources.Prefix)
// 			delimitedName := delimiter(trimmedName, resources.Delimiter)
// 			switch true {
// 			case name == resources.Prefix:
// 				// Use resources.Prefix to filter out delimited file
// 				metadata, err = getMetadata(fs.path, bucket, name)
// 				if err != nil {
// 					return ObjectMetadata{}, resources, err.Trace()
// 				}
// 				if metadata.Mode.IsDir() {
// 					resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
// 					return ObjectMetadata{}, resources, nil
// 				}
// 			case delimitedName == content.FileInfo.Name():
// 				// Use resources.Prefix to filter out delimited files
// 				metadata, err = getMetadata(fs.path, bucket, name)
// 				if err != nil {
// 					return ObjectMetadata{}, resources, err.Trace()
// 				}
// 				if metadata.Mode.IsDir() {
// 					resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
// 					return ObjectMetadata{}, resources, nil
// 				}
// 			case delimitedName != "":
// 				resources.CommonPrefixes = append(resources.CommonPrefixes, resources.Prefix+delimitedName)
// 			}
// 		}
// 	// Delimiter present and Prefix is absent
// 	case resources.Delimiter != "" && resources.Prefix == "":
// 		delimitedName := delimiter(name, resources.Delimiter)
// 		switch true {
// 		case delimitedName == "":
// 			metadata, err = getMetadata(fs.path, bucket, name)
// 			if err != nil {
// 				return ObjectMetadata{}, resources, err.Trace()
// 			}
// 			if metadata.Mode.IsDir() {
// 				resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
// 				return ObjectMetadata{}, resources, nil
// 			}
// 		case delimitedName == content.FileInfo.Name():
// 			metadata, err = getMetadata(fs.path, bucket, name)
// 			if err != nil {
// 				return ObjectMetadata{}, resources, err.Trace()
// 			}
// 			if metadata.Mode.IsDir() {
// 				resources.CommonPrefixes = append(resources.CommonPrefixes, name+resources.Delimiter)
// 				return ObjectMetadata{}, resources, nil
// 			}
// 		case delimitedName != "":
// 			resources.CommonPrefixes = append(resources.CommonPrefixes, delimitedName)
// 		}
// 	// Delimiter is absent and only Prefix is present
// 	case resources.Delimiter == "" && resources.Prefix != "":
// 		if strings.HasPrefix(name, resources.Prefix) {
// 			// Do not strip prefix object output
// 			metadata, err = getMetadata(fs.path, bucket, name)
// 			if err != nil {
// 				return ObjectMetadata{}, resources, err.Trace()
// 			}
// 		}
// 	default:
// 		metadata, err = getMetadata(fs.path, bucket, name)
// 		if err != nil {
// 			return ObjectMetadata{}, resources, err.Trace()
// 		}
// 	}
// 	sortUnique(sort.StringSlice(resources.CommonPrefixes))
// 	return metadata, resources, nil
// }
