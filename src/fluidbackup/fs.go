package fluidbackup

import "fmt"

/* Represents a data store provider. Is the entity that actually
stores information. */
type FluidServer struct {
}

func (fv *FluidServer) PrintHelloWorld() {
	fmt.Println("Hellow World")
}

// A temporary store data file that is archaic
func (fv *FluidServer) StoreData(data []byte) {

}
