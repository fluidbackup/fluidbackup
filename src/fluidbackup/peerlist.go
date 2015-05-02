package fluidbackup

import "sync"
import "math/rand"
import "time"

type PeerRequest struct {
	Bytes int
	IgnorePeers map[PeerId]bool
}

type PeerList struct {
	mu sync.Mutex
	protocol *Protocol
	peers map[PeerId]*Peer
	lastRequest *PeerRequest
}

func MakePeerList(protocol *Protocol) *PeerList {
	this := new(PeerList)
	this.protocol = protocol
	this.peers = make(map[PeerId]*Peer)
	this.lastRequest = nil

	go func() {
		for {
			this.update()
			time.Sleep(time.Duration(50 * time.Millisecond))
		}
	}()

	return this
}

func (this *PeerList) DiscoveredPeer(peerId PeerId) *Peer {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.discoveredPeer(peerId)
}

func (this *PeerList) discoveredPeer(peerId PeerId) *Peer {
	peer, ok := this.peers[peerId]
	if !ok {
		Log.Info.Printf("Registering newly discovered peer at %s", peerId.String())
		peer = MakePeer(peerId, this.protocol)
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
