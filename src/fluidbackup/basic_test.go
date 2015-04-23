package fluidbackup

import "testing"
import "time"

func TestBasic(t *testing.T) {
	b1 := MakeFluidBackup(19838)
	b2 := MakeFluidBackup(19839)
	b1.fileStore.RegisterFile("orange.txt")
	b1.peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: 19839})
	b2.peerList.DiscoveredPeer(PeerId{Address: "127.0.0.1", Port: 19838})
	time.Sleep(10 * time.Second)
}
