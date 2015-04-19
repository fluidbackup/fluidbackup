package fluidbackup

import "sync"

const (
	STATUS_ONLINE = 0
	STATUS_OFFLINE = 1
)

type PeerId struct {
	Address string
	Port int
}

type Peer struct {
	mu sync.Mutex
	protocol *Protocol
	id PeerId
	status int
	localBytes int // how many bytes we've agreed to store for this peer
	remoteBytes int // how many bytes peer is willing to store for us

	// cached values
	localUsedBytes int
	remoteUsedBytes int
}

func (this *Peer) proposeAgreement(localBytes int, remoteBytes int) bool {
	if this.protocol.proposeAgreement(this.id, localBytes, remoteBytes) {
		this.mu.Lock()
		this.localBytes += localBytes
		this.remoteBytes += remoteBytes
		this.mu.Unlock()
		return true
	}

	return false
}

/*
 * Attempts to reserve a number of bytes for storage on this peer.
 * Returns true if the bytes have been reserved for use by caller, or false if reservation failed.
 */
func (this *Peer) reserveBytes(bytes int) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.remoteBytes - this.remoteUsedBytes >= bytes {
		this.remoteUsedBytes += bytes
		return true
	} else {
		return false
	}
}

func (this *Peer) isOnline() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.status == STATUS_ONLINE
}
