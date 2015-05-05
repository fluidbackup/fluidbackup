package fluidbackup

import "crypto/md5"
import "os"
import "sync"
import "bufio"
import "strings"
import "fmt"
import "encoding/hex"
import "time"
import "io"

/*
 * FileId includes the absolute pathname of the file object.
 */
type FileId string

type File struct {
	Id     FileId
	Hash   []byte
	Length int
	Blocks []BlockId
	Ready  bool // whether we finished creating all the blocks for this file
	Error  bool // whether we had an error while reading from this file
}

/*
 * A scheduled job for the filestore to read a file.
 * This is used in two cases
 *   a) We have just received a new file registration, and need to perform the initial
 *      file read so that we can replicate the data.
 *   b) Some data has been deleted from memory and needs re-replication, so the file
 *      needs to be read again.
 */
type FileReadJob struct {
	FileId FileId
	Subs   map[int]FileReadJobSub // map of offsets to read, or nil if we are reading the entire file for the first time
}
type FileReadJobSub struct {
	BlockId BlockId
	Offset int
}

/* Represents a data store provider. Is the entity that actually
stores information. */
type FileStore struct {
	mu          sync.Mutex
	blockStore  *BlockStore
	files       map[FileId]*File // map from local filesystem path strings to file objects
	pendingJobs map[FileId]FileReadJob
}

func MakeFileStore(fluidBackup *FluidBackup, blockStore *BlockStore) *FileStore {
	this := new(FileStore)
	this.blockStore = blockStore
	this.files = make(map[FileId]*File)
	this.pendingJobs = make(map[FileId]FileReadJob)

	go func() {
		for !fluidBackup.Stopping() {
			this.update()
			time.Sleep(time.Duration(100 * time.Millisecond))
		}
	}()

	return this
}

/*
 * Registers a file at the provided local path with fluidbackup.
 * The file will be read block by block, blocks will be passed to blockstore for later distribution over peers.
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
	file := &File{}
	file.Id = FileId(path)
	file.Blocks = make([]BlockId, 0)
	file.Ready = false
	file.Error = false
	this.files[file.Id] = file

	// asynchronously read the file
	this.pendingJobs[file.Id] = FileReadJob{FileId: file.Id, Subs: nil}

	return file
}

func (this *FileStore) RequestReread(fileId FileId, offset int, blockId BlockId) {
	this.mu.Lock()
	defer this.mu.Unlock()

	pendingJob, ok := this.pendingJobs[fileId]
	if !ok {
		file := this.files[fileId]

		if file == nil {
			// perhaps this file was unregistered
			// if this wasn't a new registration, we should make sure the blocks get deleted
			//TODO
			return
		} else if file.Error {
			Log.Debug.Printf("File [%s] still in error state, unable to satisfy reread request for block %d", fileId, blockId)
			return
		}

		pendingJob = FileReadJob{
			FileId: fileId,
			Subs: make(map[int]FileReadJobSub),
		}
		this.pendingJobs[file.Id] = pendingJob
	} else if pendingJob.Subs == nil {
		// the file hasn't even been read the first time yet!?!?
		return
	}

	pendingJob.Subs[offset] = FileReadJobSub{Offset: offset, BlockId: blockId}
}

func (this *FileStore) update() {
	this.mu.Lock()
	defer this.mu.Unlock()

	// handle the next read job, if any
	for key, job := range this.pendingJobs {
		this.handleJob(job)
		delete(this.pendingJobs, key)
	}
}

func (this *FileStore) handleJob(job FileReadJob) {
	file := this.files[job.FileId]

	if file == nil {
		// perhaps this file was unregistered
		// if this wasn't a new registration, we should make sure the blocks get deleted
		//TODO
		return
	}

	path := string(file.Id)
	Log.Debug.Printf("Handling read job from file [%s]", path)

	if job.Subs == nil && file.Ready {
		// this file is marked ready, but job tells us we are reading for the first time
		Log.Warn.Printf("Skipping file read job for file already marked ready [%s]", path)
		return
	}

	fin, err := os.Open(path)

	if err != nil {
		Log.Error.Printf("Error encountered while reading from file [%s]: %s", path, err.Error())
		file.Error = true // don't attempt to read from this file again until user resolves the problem
		return
	}

	defer fin.Close()
	// we use a fixed length buffer to read the file
	// zeroing the buffer is needed for the last block of the file, since we want to
	//  make sure any extra bytes that get tacked on are consistent across re-reads of
	//  the file (otherwise we'll have hash mismatch issues)
	buf := make([]byte, FILE_BLOCK_SIZE)

	if job.Subs == nil {
		file.Length = 0
		hasher := md5.New()

		for {
			zero(buf)
			readCount, err := fin.Read(buf)

			if err != nil {
				Log.Error.Printf("Error encountered while reading from file [%s]: %s", path, err.Error())
				file.Error = true // don't attempt to read from this file again until user resolves the problem
				return
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
	} else {
		for _, sub := range job.Subs {
			zero(buf)
			_, err = fin.ReadAt(buf, int64(sub.Offset))

			if err != nil && err != io.EOF {
				Log.Error.Printf("Error encountered while reading from file [%s]: %s", path, err.Error())
				file.Error = true // don't attempt to read from this file again until user resolves the problem
				return
			}

			this.blockStore.RetrievedBlockContents(sub.BlockId, buf)
		}
	}
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
