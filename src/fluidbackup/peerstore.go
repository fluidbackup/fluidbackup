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

type PeerStore struct {
	peerAgreements []PeerAgreement
}

/*
 * Attempts to identify an available peer.
 * Caller requires [bytes] free bytes to use and doesn't want the peer to be in [ignorePeers] list.
 * Returns nil if we aren't able to satisfy the request (in this case we should also launch task to get more space).
 */
func (peerStore *PeerStore) FindAvailablePeer(bytes int, ignorePeers []Peer) *Peer {
	return nil
}
