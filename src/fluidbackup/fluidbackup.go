package fluidbackup

import "fmt"

/* This is the entry point into the fluidbackup system,
and provides the interface for clients to interact with said system. */
type FluidBackup struct {
}

func (fb *FluidBackup) PrintTrueHelloWorld() {
	fmt.Println("Hello World")
}
