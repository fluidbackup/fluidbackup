package fluidbackup

import "crypto/md5"
import "os"
import "sync"
import "bufio"
import "strings"
import "fmt"
import "encoding/hex"

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
		blockId := this.blockStore.RegisterBlock(file.Id, file.Length, buf) // file.Length used as block's offset in parent file
		file.Length += readCount
		file.Blocks = append(file.Blocks, blockId)

		if readCount < FILE_BLOCK_SIZE {
			break
		}
	}

	file.Hash = hasher.Sum(nil)

	// update files structure
	Log.Info.Printf("Registered new file from [%s]", path)
	this.files[file.Id] = file
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

/*
 * filestore metadata can be written and read from the disk using Save/Load functions below.
 * The file format is a file on each line, consisting of string:
 *     [filename]:[length]:[hex(hash)]:[block1],[block2],...,[block n],
 */

func (this *FileStore) Save() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	Log.Info.Printf("Saving metadata to filestore.dat (%d files)", len(this.files))
	fout, err := os.Create("filestore.dat")
	if err != nil {
		Log.Warn.Printf("Failed to save metadata to filestore.dat: %s", err.Error())
		return false
	}
	defer fout.Close()

	for fileId, file := range this.files {
		fileDump := fmt.Sprintf("%s:%d:%s:", fileId, file.Length, hex.EncodeToString(file.Hash))
		for _, blockId := range file.Blocks {
			fileDump += fmt.Sprintf("%d", blockId) + ","
		}
		fileDump += "\n"
		fout.Write([]byte(fileDump))
	}

	return true
}

func (this *FileStore) Load() bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	fin, err := os.Open("filestore.dat")
	if err != nil {
		Log.Warn.Printf("Failed to read metadata from filestore.dat: %s", err.Error())
		return false
	}
	defer fin.Close()

	scanner := bufio.NewScanner(fin)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ":")

		if len(parts) != 4 {
			continue
		}

		file := &File{}
		file.Id = FileId(parts[0])
		file.Length = strToInt(parts[1])
		file.Hash, _ = hex.DecodeString(parts[2])

		blockStrings := strings.Split(parts[3], ",")
		file.Blocks = make([]BlockId, len(blockStrings) - 1) // last element of blockStrings is empty
		for i, blockString := range blockStrings {
			if i < len(file.Blocks) {
				file.Blocks[i] = BlockId(strToInt64(blockString))
			}
		}

		this.files[file.Id] = file
	}

	Log.Info.Printf("Loaded %d files", len(this.files))
	return true
}
