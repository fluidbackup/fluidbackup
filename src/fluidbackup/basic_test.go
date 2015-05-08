package fluidbackup

import "testing"
import "time"
import "io/ioutil"
import "math/rand"
import "os"
import "bytes"
import "strings"
import "fmt"

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

	// check the number of discovered peers for one on centroid
	numPeers := newDiscoverer.peerList.NumPeers()
	desiredNum := PeerListDefaultDesiredNumPeers
	fmt.Printf("Discoverer Found %v peers!\n", numPeers)
	if numPeers < desiredNum {
		t.Fatalf("Num peers %v, expected at least %v", numPeers, desiredNum)
	}

	// check the number of discovered peers for each bOther
	for i := range bOther {
		fbOther := bOther[i]
		numPeers := fbOther.peerList.NumPeers()
		fmt.Printf("%v Found %v peers!\n", fbOther.protocol.port, numPeers)
		if numPeers < desiredNum {
			t.Fatalf("%v: Num peers %v, expected at least %v", fbOther.protocol.port, numPeers, desiredNum)
		}
	}

	// shut down fluidbackup instances
	bMain.Stop()
	newDiscoverer.Stop()
	for _, inst := range bOther {
		inst.Stop()
	}
}

func killForSeconds(fb *FluidBackup, port int, seconds int) {
	// Kill our discoverer
	fb.Save()
	fb.Stop()
	time.Sleep(time.Duration(seconds) * time.Second)
	fb = MakeFluidBackup(port)
	fb.Load()
}

// Does a very simple test to see if fluidbackup
// shifts its shard storage in response to a change
// in trustworthiness of peers. This does not directly
// test the trust mechanism but may instead test the
// next available peer mechanism, but should pass.
func TestBasicTrustMechanism(t *testing.T) {
	fileContents := make([]byte, 1500*1024)
	for i := range fileContents {
		fileContents[i] = byte(rand.Int() % 256)
	}
	err := ioutil.WriteFile("orange.txt", fileContents, 0644)
	if err != nil {
		t.Error("failed to write random contents to orange.txt: " + err.Error())
	}

	// Build our initial network
	mainPort := 19838
	bMain := MakeFluidBackup(mainPort)
	bOther := make([]*FluidBackup, 11)
	for i := range bOther {
		port := 19839 + i
		bOther[i] = MakeFluidBackup(port)
		// build an initial ring structure
		bOther[i].peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: port - 1})
	}
	discoverer := MakeFluidBackup(mainPort - 1)
	discoverer.peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: mainPort})

	discoverer.fileStore.RegisterFile("orange.txt")

	// Make some peers less trustworthy by killing them for a while
	for i := 0; i < 5; i++ {
		killForSeconds(bOther[i], 19839+i, 2)
	}

	time.Sleep(5 * time.Second)
	fmt.Printf("NUM PEERS: %v\n", discoverer.peerList.NumPeers())

	err = os.Remove("orange.txt")
	if err != nil {
		t.Error("Encountered error while removing file: " + err.Error())
		return
	}

	// wait for equilibrium
	time.Sleep(3 * time.Second)
	// Kill these less trustworthy peers permanently
	// the shards should be on the more trustworthy peers.
	for i := 0; i < 5; i++ {
		bOther[i].Stop()
	}

	time.Sleep(3 * time.Second)

	killForSeconds(discoverer, mainPort-1, 5)

	// Attempt to recover the file
	discoverer.fileStore.RecoverFile("orange.txt")
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

func TestBenchmark(t *testing.T) {

}
