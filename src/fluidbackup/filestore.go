package fluidbackup

import "crypto/md5"
import "os"
import "sync"

/*
 * FileId includes the absolute pathname of the file object.
 */
type FileId string

type File struct {
	Id FileId
	Hash []byte
	Length int
	Blocks []BlockId
}

/* Represents a data store provider. Is the entity that actually
stores information. */
type FileStore struct {
	mu sync.Mutex
	blockStore *BlockStore
	files map[FileId]*File // map from local filesystem path strings to file objects
}

func MakeFileStore(blockStore *BlockStore) *FileStore {
	this := new(FileStore)
	this.blockStore = blockStore
	this.files = make(map[FileId]*File)
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
	_, alreadyHave := this.files[FileId(path)]
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
	file.Id = FileId(path)
	file.Blocks = make([]BlockId, 0)
	hasher := md5.New()

	for {
		readCount, err := f.Read(buf)

		if err != nil {
			Log.Warn.Printf("Error encountered while registering file [%s]: %s", path, err.Error())
			return nil
		}

		hasher.Write(buf)
		blockId := this.blockStore.RegisterBlock(path, file.Length, buf) // file.Length used as block's offset in parent file
		file.Length += readCount
		file.Blocks = append(file.Blocks, blockId)

		if readCount < FILE_BLOCK_SIZE {
			break
		}
	}

	file.Hash = hasher.Sum(nil)

	// update files structure
	Log.Info.Printf("Registered new file from [%s]", path)
	this.files[FileId(path)] = file
	return file
}

func (this *FileStore) RecoverFile(path string) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	// make sure file exists
	file := this.files[FileId(path)]
	if file == nil {
		return false
	}

	// recover file block by block
	Log.Info.Printf("Recovering file [%s]", path)
	fout, err := os.Create(path)
	if err != nil {
		Log.Warn.Printf("Error encountered while opening file in write mode for recovery [%s]: %s", path, err.Error())
		return false
	}

	defer fout.Close()
	numWritten := 0

	for _, blockId := range file.Blocks {
		blockBytes := this.blockStore.RecoverBlock(blockId)

		if blockBytes == nil {
			return false
		}

		if numWritten + len(blockBytes) > file.Length {
			blockBytes = blockBytes[:file.Length - numWritten]
		}

		_, err := fout.Write(blockBytes)

		if err != nil {
			Log.Warn.Printf("Error encountered while writing to file during recovery [%s]: %s", path, err.Error())
			return false
		}

		numWritten += len(blockBytes)
	}

	return true
}
