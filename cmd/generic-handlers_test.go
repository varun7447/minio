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
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	router "github.com/gorilla/mux"
)

// Tests getRedirectLocation function for all its criteria.
func TestRedirectLocation(t *testing.T) {
	testCases := []struct {
		urlPath  string
		location string
	}{
		{
			// 1. When urlPath is '/minio'
			urlPath:  minioReservedBucketPath,
			location: minioReservedBucketPath + "/",
		},
		{
			// 2. When urlPath is '/'
			urlPath:  "/",
			location: minioReservedBucketPath + "/",
		},
		{
			// 3. When urlPath is '/webrpc'
			urlPath:  "/webrpc",
			location: minioReservedBucketPath + "/webrpc",
		},
		{
			// 4. When urlPath is '/login'
			urlPath:  "/login",
			location: minioReservedBucketPath + "/login",
		},
		{
			// 5. When urlPath is '/favicon.ico'
			urlPath:  "/favicon.ico",
			location: minioReservedBucketPath + "/favicon.ico",
		},
		{
			// 6. When urlPath is '/unknown'
			urlPath:  "/unknown",
			location: "",
		},
	}

	// Validate all conditions.
	for i, testCase := range testCases {
		loc := getRedirectLocation(testCase.urlPath)
		if testCase.location != loc {
			t.Errorf("Test %d: Unexpected location expected %s, got %s", i+1, testCase.location, loc)
		}
	}
}

// Tests browser request guess function.
func TestGuessIsBrowser(t *testing.T) {
	if guessIsBrowserReq(nil) {
		t.Fatal("Unexpected return for nil request")
	}
	r := &http.Request{
		Header: http.Header{},
	}
	r.Header.Set("User-Agent", "Mozilla")
	if !guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't fail for a possible browser request.")
	}
	r = &http.Request{
		Header: http.Header{},
	}
	r.Header.Set("User-Agent", "mc")
	if guessIsBrowserReq(r) {
		t.Fatal("Test shouldn't report as browser for a non browser request.")
	}
}

var isHTTPHeaderSizeTooLargeTests = []struct {
	header     http.Header
	shouldFail bool
}{
	{header: generateHeader(0, 0), shouldFail: false},
	{header: generateHeader(1024, 0), shouldFail: false},
	{header: generateHeader(2048, 0), shouldFail: false},
	{header: generateHeader(8*1024+1, 0), shouldFail: true},
	{header: generateHeader(0, 1024), shouldFail: false},
	{header: generateHeader(0, 2048), shouldFail: true},
	{header: generateHeader(0, 2048+1), shouldFail: true},
}

func generateHeader(size, usersize int) http.Header {
	header := http.Header{}
	for i := 0; i < size; i++ {
		header.Add(strconv.Itoa(i), "")
	}
	userlength := 0
	for i := 0; userlength < usersize; i++ {
		userlength += len(userMetadataKeyPrefixes[0] + strconv.Itoa(i))
		header.Add(userMetadataKeyPrefixes[0]+strconv.Itoa(i), "")
	}
	return header
}

func TestIsHTTPHeaderSizeTooLarge(t *testing.T) {
	for i, test := range isHTTPHeaderSizeTooLargeTests {
		if res := isHTTPHeaderSizeTooLarge(test.header); res != test.shouldFail {
			t.Errorf("Test %d: Expected %v got %v", i, res, test.shouldFail)
		}
	}
}

// TestLoggingHandler - test for logging handler.
func TestLoggingHandler(t *testing.T) {
	logDir, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(logDir)

	// Set trace dir
	globalHTTPTraceDir = logDir
	defer func() {
		globalHTTPTraceDir = ""
	}()

	// Setup object REST API handlers.
	apiRouter := router.NewRouter()
	registerAPIRouter(apiRouter)

	// Prepare server with FS backend
	loggingHandlerTestBed, err := prepareFSTestServer(apiRouter)
	if err != nil {
		t.Fatal("Failed to initialize a single node FS backend for logging handler tests.")
	}
	defer loggingHandlerTestBed.TearDown()

	bucketName := getRandomBucketName()
	objectName := getRandomObjectName()

	data := []byte(`Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.`)

	// Create bucket
	loggingHandlerTestBed.objLayer.MakeBucketWithLocation(bucketName, "")

	cred := serverConfig.GetCredential()

	// Prepare query params for get-config mgmt REST API.
	req, err := newTestSignedRequestV4("PUT", getPutObjectURL("", bucketName, objectName),
		int64(len(data)), bytes.NewReader(data),
		cred.AccessKey, cred.SecretKey)
	if err != nil {
		t.Fatalf("Failed to create HTTP request for Put Object: <ERROR> %v", err)
	}

	setupHTTPTrace()
	defer globalHTTPTrace.logFile.Close()

	// Run a PUT object call with logging Handler
	handler := registerHandlers(apiRouter, setLoggingHandler)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// We expect ot have a successful response
	if rec.Code != http.StatusOK {
		t.Errorf("Expected to succeed but failed with %d", rec.Code)
	}

	// Fetch log file name as it is variable by time
	logFile := ""
	files, _ := ioutil.ReadDir(logDir)
	for _, file := range files {
		logFile = filepath.Join(logDir, file.Name())
		break
	}

	// Load log file content
	logFileContentBytes, err := ioutil.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	logFileContentsStr := string(logFileContentBytes)

	// Check if log file contains correct PUT call
	if !strings.Contains(logFileContentsStr, "PUT /"+bucketName+"/"+objectName) {
		t.Fatal("Cannot find correct PUT in log test")
	}

	// We log the string "[BODY]" instead of the actual body.
	if !strings.Contains(logFileContentsStr, "[BODY]") {
		t.Fatal(`log file should contain the body place holder string "[BODY]"`)
	}
}
