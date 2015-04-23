package fluidbackup

import "crypto/md5"
import "os"
import "sync"

type File struct {
	LocalPath string
	Hash []byte
	Length int
	Blocks []*Block
}

/* Represents a data store provider. Is the entity that actually
stores information. */
type FileStore struct {
	mu sync.Mutex
	blockStore *BlockStore
	files map[string]*File // map from local filesystem path strings to file objects
}

func MakeFileStore(blockStore *BlockStore) *FileStore {
	this := new(FileStore)
	this.blockStore = blockStore
	this.files = make(map[string]*File)
	return this
}

/*
 * Registers a file at the provided local path with fluidbackup.
 * The file is read block by block, blocks will be passed to blockstore for later distribution over peers.
 */
func (this *FileStore) RegisterFile(path string) *File {
	this.mu.Lock()
	defer this.mu.Unlock()

	// make sure we don't already have a record of this file
	_, alreadyHave := this.files[path]
	if alreadyHave {
		return nil
	}

	Log.Info.Printf("Registering new file [%s]", path)
	f, err := os.Open(path)

	if err != nil {
		Log.Warn.Printf("Error encountered while registering file [%s]: %s", path, err.Error())
		return nil
	}

	defer f.Close()
	buf := make([]byte, FILE_BLOCK_SIZE)
	file := &File{}
	file.LocalPath = path
	file.Blocks = make([]*Block, 0)
	hasher := md5.New()

	for {
		readCount, err := f.Read(buf)

		if err != nil {
			Log.Warn.Printf("Error encountered while registering file [%s]: %s", path, err.Error())
			return nil
		}

		hasher.Write(buf)
		block := this.blockStore.RegisterBlock(path, file.Length, buf) // file.Length used as block's offset in parent file
		file.Length += readCount

		if block == nil {
			Log.Warn.Printf("Failed to create block while registering file [%s]", path)
			return nil
		}

		file.Blocks = append(file.Blocks, block)

		if readCount < FILE_BLOCK_SIZE {
			break
		}
	}

	file.Hash = hasher.Sum(nil)

	// update files structure
	Log.Info.Printf("Registered new file from [%s]", path)
	this.files[path] = file
	return file
}
