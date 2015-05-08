package fluidbackup

import "testing"
import "time"
import "io/ioutil"
import "math/rand"
import "os"
import "bytes"
import "strings"

func TestBasic(t *testing.T) {
	fileContents := make([]byte, 1500*1024)
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
			bOther[i].peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort})
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

	// shut down fluidbackup instances
	bMain.Stop()
	for _, inst := range bOther {
		inst.Stop()
	}
}

// Attempts to kill the local peer
// and ensure that the file can be recovered.
func TestStable(t *testing.T) {
	fileContents := make([]byte, 1500*1024)
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
			bOther[i].peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort})
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

	// shut down fluidbackup instances
	bMain.Stop()
	for _, inst := range bOther {
		inst.Stop()
	}
}

func TestReread(t *testing.T) {
	fileContents := make([]byte, 1500*1024)
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
	time.Sleep(time.Second)
	bMain.Save()
	bMain.Stop()
	time.Sleep(2 * time.Second)
	bMain = MakeFluidBackup(mainPort)
	bMain.Load()

	bOther := make([]*FluidBackup, 11)
	for i := range bOther {
		port := 19839 + i
		bOther[i] = MakeFluidBackup(port)

		if i < 5 {
			bMain.peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: port})
		} else {
			bOther[i].peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort})
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

	// shut down fluidbackup instances
	bMain.Stop()
	for _, inst := range bOther {
		inst.Stop()
	}
}

func TestFailVerify(t *testing.T) {
	fileContents := make([]byte, 1500*1024)
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
			bOther[i].peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort})
		}
	}

	time.Sleep(5 * time.Second)
	files, _ := ioutil.ReadDir("store/")
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".obj") {
			os.Remove("store/" + f.Name())
		}
	}
	time.Sleep(25 * time.Second)
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

	// shut down fluidbackup instances
	bMain.Stop()
	for _, inst := range bOther {
		inst.Stop()
	}
}

func TestBasicPeerSharing(t *testing.T) {
	// todo: need to implement:
	// - trust ranking
	// - how many peers do we want?
	mainPort := 19838
	bMain := MakeFluidBackup(mainPort)

	// Make a giant centroid peer network first
	bOther := make([]*FluidBackup, 11)
	for i := range bOther {
		port := 19839 + i
		bOther[i] = MakeFluidBackup(port)
		if i < 5 {
			bMain.peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: port})
		} else {
			bOther[i].peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort})
		}
	}

	time.Sleep(time.Second)

	// Now, connect anoter peer to the centroid
	newDiscoverer := MakeFluidBackup(mainPort - 1)
	newDiscoverer.peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort})
	// No need for next line: they are now autodiscovered
	// numPeers := newDiscoverer.peerList.FindNewPeers(len(bOther))

	time.Sleep(time.Second * 5)

	numPeers := newDiscoverer.peerList.NumPeers()
	desiredNum := PeerListDefaultDesiredNumPeers
	if numPeers != desiredNum {
		t.Fatalf("Num peers %v, expected %v", numPeers, desiredNum)
	}

	// shut down fluidbackup instances
	bMain.Stop()
	newDiscoverer.Stop()
	for _, inst := range bOther {
		inst.Stop()
	}
}
