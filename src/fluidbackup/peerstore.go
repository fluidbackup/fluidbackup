package fluidbackup

type PeerAgreement struct {
	// stable values
	agreementId string
	peer Peer // peer with whom we have this agreement
	localBytes int // how many bytes we're storing for the peer
	remoteBytes int // how many bytes the peer is storing for us

	// cached values
	localUsedBytes int
	remoteUsedBytes int
}

type Peer struct {
	Id string
}

const (
	STATUS_ONLINE = 0
	STATUS_OFFLINE = 1
)

type PeerStatus struct {
	status int
}

type PeerRequest struct {
	Bytes int
	IgnorePeers map[Peer]bool
	KnownPeers map[Peer]PeerStatus
}

type PeerStore struct {
	mu sync.Mutex
	protocol *Protocol
	peerAgreements map[Peer]*PeerAgreement
	lastRequest *PeerRequest
}

func MakePeerStore(protocol *Protocol) *PeerStore {
	this := new(PeerStore)
	this.protocol = protocol
	this.peerAgreements = make(map[Peer]*PeerAgreement)
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
func (this *PeerStore) FindAvailablePeer(bytes int, ignorePeers map[Peer]bool) *Peer {
	this.mu.Lock()
	defer this.mu.Unlock()

	for peer, agreement := range this.peerAgreements {
		_, shouldIgnore := ignorePeers[peer]
		if !shouldIgnore && agreement.remoteBytes - agreement.remoteUsedBytes >= bytes {
			// this one works, has enough bytes and not in ignorePeers
			agreement.remoteUsedBytes += bytes
			return peer
		}
	}

	// none of our existing agreements satisfied this request
	// update last request so that we eventually satisfy
	lastRequest = &PeerRequest{Bytes: bytes, IgnorePeers: ignorePeers}
}

func (this *PeerStore) update() {
	this.mu.Lock()

	// handle last failed request if set
	if this.lastRequest != nil {
		request := *this.lastRequest
		this.lastRequest = nil
		this.mu.Unlock()
		createAgreementSatisfying(request)
		return
	}

	this.mu.Unlock()
}

func (this *PeerStore) createAgreementSatisfying(request PeerRequest) {
	// pick random online peer that isn't in the ignore list
	this.mu.Lock()
	possiblePeers = make([]Peer)
	for peer, status := range this.knownPeers {
		_, shouldIgnore := request.ignorePeers[peer]
		if !shouldIgnore && status.status == STATUS_ONLINE {
			possiblePeers = append(possiblePeers, peer)
		}
	}
	randomPeer := possiblePeers[rand.Intn(len(possiblePeers))]
	this.mu.Unlock() // don't want to hold lock during long-lived peer communication

	if this.protocol.proposeAgreement(randomPeer, request.bytes, request.bytes) {
		this.mu.Lock()
		// update our agreement table to match the accepted agreement
		_, existingAgreement = this.peerAgreements[randomPeer]

		if existingAgreement {
			existingAgreement.remoteBytes += request.bytes
			existingAgreement.localBytes += request.bytes
		} else {
			this.peerAgreements[randomPeer] = PeerAgreement{agreementId: randSeq(16), peer: randomPeer, localBytes: request.bytes, remoteBytes: request.bytes}
		}
		this.mu.Unlock()
	}
}
