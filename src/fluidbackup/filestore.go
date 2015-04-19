package fluidbackup

import "fmt"
import "crypto/md5"
import "os"

type File struct {
	localPath string
	hash []byte
	length int
	blocks []Block
}

/* Represents a data store provider. Is the entity that actually
stores information. */
type FileStore struct {
	files map[string]*File // map from local filesystem path strings to file objects
}

// A temporary store data file that is archaic
func (this *FileStore) StoreData(path string, data []byte) {

}

/*
 * Registers a file at the provided local path with fluidbackup.
 * The file is read block by block, blocks will be passed to blockstore for eventual distribution.
 *
 */
func (this *FileStore) RegisterFile(path string) bool {
	f, err := os.Open(path)

	if err != nil {
		Log.Warn.Printf("Error encountered while registering file [%s]: %s", path, err.Error())
		return
	}

	defer f.Close()
	buf := make([]byte, FILE_BLOCK_SIZE)

	for {
		readCount, err := f.Read(buf)

		if err != nil {
			Log.Warn.Printf("Error encountered while registering file [%s]: %s", path, err.Error())
			return
		}

		// ... TODO

		if readCount < FILE_BLOCK_SIZE {
			break
		}
	}
}
