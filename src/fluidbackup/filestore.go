package fluidbackup

import "fmt"

/* Represents a data store provider. Is the entity that actually
stores information. */
type FileStore struct {

}

// A temporary store data file that is archaic
func (this *FileStore) StoreData(path string, data []byte) {

}
