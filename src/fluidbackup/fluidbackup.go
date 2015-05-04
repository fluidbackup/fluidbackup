package fluidbackup

import "log"
import "io"
import "os"
import "sync/atomic"

var Log struct {
	Debug *log.Logger
	Info *log.Logger
	Warn *log.Logger
	Error *log.Logger
}

var Debug bool

func InitLogging(debugHandle io.Writer, infoHandle io.Writer, warnHandle io.Writer, errorHandle io.Writer) {
	Log.Debug = log.New(debugHandle, "DEBUG: ", log.Ldate | log.Ltime | log.Lshortfile)
	Log.Info = log.New(infoHandle, "INFO: ", log.Ldate | log.Ltime | log.Lshortfile)
	Log.Warn = log.New(warnHandle, "WARN: ", log.Ldate | log.Ltime | log.Lshortfile)
	Log.Error = log.New(errorHandle, "ERROR: ", log.Ldate | log.Ltime | log.Lshortfile)
	Debug = true
}

/* This is the entry point into the fluidbackup system,
and provides the interface for clients to interact with said system. */
type FluidBackup struct {
	peerList *PeerList
	fileStore *FileStore
	blockStore *BlockStore
	protocol *Protocol

	stopping *int32
}

func MakeFluidBackup(port int) *FluidBackup {
	InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
	this := new(FluidBackup)

	this.stopping = new(int32)
	this.protocol = MakeProtocol(this, port)
	this.peerList = MakePeerList(this, this.protocol)
	this.protocol.setPeerList(this.peerList)

	this.blockStore = MakeBlockStore(this, this.peerList)
	this.fileStore = MakeFileStore(this, this.blockStore)
	this.blockStore.setFileStore(this.fileStore)

	return this
}

func (this *FluidBackup) Save() bool {
	return this.fileStore.Save() && this.blockStore.Save() && this.peerList.Save()
}

func (this *FluidBackup) Load() bool {
	return this.peerList.Load() && this.blockStore.Load() && this.fileStore.Load()
}

func (this *FluidBackup) Stop() {
	Log.Info.Printf("Stopping...")
	atomic.StoreInt32(this.stopping, 1)
	this.protocol.Stop()
}

func (this *FluidBackup) Stopping() bool {
	return atomic.LoadInt32(this.stopping) != 0
}
