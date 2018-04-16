package cmd

import (
	"io/ioutil"
	"log"
	"testing"
)

func TestBitrotReaderWriter(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatal(err)
	}
	// defer os.RemoveAll(tmpDir)

	volume := "testvol"
	filePath := "testfile"

	disk, err := newPosix(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	disk.MakeVol(volume)

	writer := NewBitrotWriter(disk, volume, filePath, HighwayHash256G, 10)

	// Simulate Append() to handle all corner cases.
	err = writer.Append([]byte("aaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	err = writer.Append([]byte("a"))
	if err != nil {
		log.Fatal(err)
	}
	err = writer.Append([]byte("aaaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	err = writer.Append([]byte("aaaaa"))
	if err != nil {
		log.Fatal(err)
	}
	err = writer.Append([]byte("aaaaaaaaaa"))
	if err != nil {
		log.Fatal(err)
	}

	sums := writer.Close()

	reader := NewBitrotReader(disk, volume, filePath, HighwayHash256G, 10, sums, 35)
	buf := make([]byte, 34)
	if err = reader.Read(buf, 0); err != nil {
		log.Fatal(err)
	}
}
