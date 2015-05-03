package fluidbackup

import "testing"
import "time"
import "io/ioutil"
import "math/rand"
import "os"
import "bytes"

func TestBasic(t *testing.T) {
	fileContents := make([]byte, 1500 * 1024)
	for i := range fileContents {
		fileContents[i] = byte(rand.Int() % 256)
	}
	err := ioutil.WriteFile("orange.txt", fileContents, 0644)
	if err != nil {
		t.Error("failed to write random contents to orange.txt: " + err.Error())
	}

	mainPort := 19838
	bMain := MakeFluidBackup(mainPort)
	bMain.fileStore.RegisterFile("orange.txt")
	bOther := make([]*FluidBackup, 11)
	for i := range bOther {
		port := 19839 + i
		bOther[i] = MakeFluidBackup(port)

		if i < 5 {
			bMain.peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: port})
		} else {
			bOther[i].peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort});
		}
	}

	time.Sleep(5 * time.Second)

	err = os.Remove("orange.txt")

	if err != nil {
		t.Error("Encountered error while removing file: " + err.Error())
		return
	}

	bMain.fileStore.RecoverFile("orange.txt")
	newFileContents, err := ioutil.ReadFile("orange.txt")
	if err != nil {
		t.Error("failed to read from recovered orange.txt: " + err.Error())
	}

	if !bytes.Equal(fileContents, newFileContents) {
		t.Error("recovered file does not match original")
	}
}

func TestStable(t *testing.T) {
	fileContents := make([]byte, 1500 * 1024)
	for i := range fileContents {
		fileContents[i] = byte(rand.Int() % 256)
	}
	err := ioutil.WriteFile("orange.txt", fileContents, 0644)
	if err != nil {
		t.Error("failed to write random contents to orange.txt: " + err.Error())
	}

	mainPort := 19838
	bMain := MakeFluidBackup(mainPort)
	bMain.fileStore.RegisterFile("orange.txt")
	bOther := make([]*FluidBackup, 11)
	for i := range bOther {
		port := 19839 + i
		bOther[i] = MakeFluidBackup(port)

		if i < 5 {
			bMain.peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: port})
		} else {
			bOther[i].peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort});
		}
	}

	time.Sleep(5 * time.Second)

	err = os.Remove("orange.txt")

	if err != nil {
		t.Error("Encountered error while removing file: " + err.Error())
		return
	}

	bMain.Save()
	bMain.Stop()
	time.Sleep(5 * time.Second)
	bMain = MakeFluidBackup(mainPort)
	bMain.Load()

	bMain.fileStore.RecoverFile("orange.txt")
	newFileContents, err := ioutil.ReadFile("orange.txt")
	if err != nil {
		t.Error("failed to read from recovered orange.txt: " + err.Error())
	}

	if !bytes.Equal(fileContents, newFileContents) {
		t.Error("recovered file does not match original")
	}
}
