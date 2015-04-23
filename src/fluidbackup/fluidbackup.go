package fluidbackup

import "log"
import "io"
import "os"

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

}

func MakeFluidBackup() *FluidBackup {
	InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
	this := new(FluidBackup)
	return this
}
