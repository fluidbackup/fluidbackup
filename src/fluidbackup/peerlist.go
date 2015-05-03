package fluidbackup

import "sync"
import "math/rand"
import "time"
import "os"
import "fmt"
import "bufio"
import "strings"

type PeerRequest struct {
	Bytes       int
	IgnorePeers map[PeerId]bool
}

/*
 * Module maintaining a list of
 * current peers involved in the distributed
 * storage scheme, and operations involving
 * all the peers (mainly storing files)
 */
type PeerList struct {
	mu          sync.Mutex
	protocol    *Protocol
	peers       map[PeerId]*Peer
	lastRequest *PeerRequest
	fluidBackup *FluidBackup
}

/*
 * Constructor
 */
func MakePeerList(fluidBackup *FluidBackup, protocol *Protocol) *PeerList {
	this := new(PeerList)
	this.fluidBackup = fluidBackup
	this.protocol = protocol
	this.peers = make(map[PeerId]*Peer)
	this.lastRequest = nil

	go func() {
		for !fluidBackup.Stopping() {
			this.update()
			time.Sleep(time.Duration(50 * time.Millisecond))
		}
	}()

	return this
}

/*
 * Add a peer to the peerList. Handles duplicates.
 */
func (this *PeerList) DiscoveredPeer(peerId PeerId) *Peer {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.discoveredPeer(peerId)
}

func (this *PeerList) discoveredPeer(peerId PeerId) *Peer {
	peer, ok := this.peers[peerId]
	if !ok {
		Log.Info.Printf("Registering newly discovered peer at %s", peerId.String())
		// Make local representations of known remote peers.
		peer = MakePeer(peerId, this.fluidBackup, this.protocol)
		this.peers[peerId] = peer
	}
	return peer
}

/*
 * Attempts to identify an available peer.
 * Caller requires [bytes] free bytes to use and doesn't want the peer to be in [ignorePeers] list.
 * Returns nil if we aren't able to satisfy the request (in this case we should also launch task to get more space).
 */
func (this *PeerList) FindAvailablePeer(bytes int, ignorePeers map[PeerId]bool) *Peer {
	this.mu.Lock()
	defer this.mu.Unlock()

	for peerId, peer := range this.peers {
		_, shouldIgnore := ignorePeers[peerId]
		if !shouldIgnore && peer.reserveBytes(bytes) {
			return peer
		}
	}

	// none of our peers could satisfy this request with existing agreements
	// update last request so that we will create new agreement and eventually satisfy
	this.lastRequest = &PeerRequest{Bytes: bytes, IgnorePeers: ignorePeers}
	return nil
}

func (this *PeerList) update() {
	this.mu.Lock()

	// handle last failed request if set
	if this.lastRequest != nil {
		request := *this.lastRequest
		this.mu.Unlock()
		this.createAgreementSatisfying(request)

		// reset last request
		// we do this after createAgreementSatisfying so that we guarantee earlier requests aren't re-satisfied
		this.mu.Lock()
		this.lastRequest = nil
		this.mu.Unlock()
		return
	}

	this.mu.Unlock()
}

func (this *PeerList) createAgreementSatisfying(request PeerRequest) {
	// pick random online peer that isn't in the ignore list
	this.mu.Lock()
	possiblePeers := make([]*Peer, 0)
	for peerId, peer := range this.peers {
		_, shouldIgnore := request.IgnorePeers[peerId]
		if !shouldIgnore && peer.isOnline() {
			possiblePeers = append(possiblePeers, peer)
		}
	}
	this.mu.Unlock() // don't want to hold lock during long-lived peer communication

	if len(possiblePeers) == 0 {
		return
	}

	randomPeer := possiblePeers[rand.Intn(len(possiblePeers))]
	// Use our "remote peer handler" object to initiate
	// agreement with the said remote peer.
	randomPeer.proposeAgreement(request.Bytes, request.Bytes)
}

func (this *PeerList) HandleProposeAgreement(peerId PeerId, localBytes int, remoteBytes int) bool {
	// don't store more than peer
	if localBytes > remoteBytes {
		return false
	}

	this.mu.Lock()
	defer this.mu.Unlock()
	// do other checking to see if proposal is useful for us...
	// TODO
	// ...

	// okay, we've accepted the proposal
	peer := this.discoveredPeer(peerId)
	peer.eventAgreement(localBytes, remoteBytes)

	return true
}

func (this *PeerList) HandleStoreShard(peerId PeerId, label int64, bytes []byte) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	peer, ok := this.peers[peerId]
	if !ok {
		return false
	}

	return peer.eventStoreShard(label, bytes)
}

func (this *PeerList) HandleRetrieveShard(peerId PeerId, label int64) []byte {
	this.mu.Lock()
	defer this.mu.Unlock()

	peer, ok := this.peers[peerId]
	if !ok {
		return nil
	}
	return peer.eventRetrieveShard(label)
}

/*
 * Peer metadata can be written and read from the disk using Save/Load functions below.
 * The file format is a peer on each line, consisting of string:
 *     [peerid]\[localBytes]\[remoteBytes]
 */

func (this *PeerList) Save() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	Log.Info.Printf("Saving peer data to peerlist.dat (%d peers)", len(this.peers))
	fout, err := os.Create("peerlist.dat")
	if err != nil {
		Log.Warn.Printf("Failed to save peer data to peerlist.dat: %s", err.Error())
		return false
	}
	defer fout.Close()

	for _, peer := range this.peers {
		fout.Write([]byte(fmt.Sprintf("%s\\%d\\%d\n", peer.id.String(), peer.localBytes, peer.remoteBytes)))
	}

	return true
}

func (this *PeerList) Load() bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	fin, err := os.Open("peerlist.dat")
	if err != nil {
		Log.Warn.Printf("Failed to read peer data from peerlist.dat: %s", err.Error())
		return false
	}
	defer fin.Close()

	scanner := bufio.NewScanner(fin)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "\\")

		if len(parts) != 3 {
			continue
		}

		peer := this.discoveredPeer(strToPeerId(parts[0]))
		peer.localBytes = strToInt(parts[1])
		peer.remoteBytes = strToInt(parts[2])
	}

	Log.Info.Printf("Loaded %d peers", len(this.peers))
	return true
}
