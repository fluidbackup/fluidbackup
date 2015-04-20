package fluidbackup

import "sync"
import "math/rand"

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
		}
	}()

	return this
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
		this.lastRequest = nil
		this.mu.Unlock()
		this.createAgreementSatisfying(request)
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
	peer, ok := this.peers[peerId]
	if !ok {
		peer := MakePeer(peerId, this.protocol)
		this.peers[peerId] = peer
	}
	peer.eventAgreement(localBytes, remoteBytes)

	return true
}
