package fluidbackup

import "sync"
import "fmt"
import "io/ioutil"
import "time"
import "math/rand"
import "strings"

const (
	STATUS_ONLINE  = 0
	STATUS_OFFLINE = 1
)

type PeerId struct {
	Address string
	Port    int
}

func (this *PeerId) String() string {
	return fmt.Sprintf("%s:%d", this.Address, this.Port)
}

func strToPeerId(str string) PeerId {
	parts := strings.Split(str, ":")
	return PeerId{
		Address: parts[0],
		Port:    strToInt(parts[1]),
	}
}

/*
 * Represents another peer, storing information about
 * the other peer as necessary, and handling requests/actions
 * involving that other peer (storeShard, etc.)
 *
 * Note: Does not represent the local peer. The local peer
 * is perhaps best represented by a combination of Protocol,
 * and PeerList, and FileStorage, which comprise a system.
 */
type Peer struct {
	mu          sync.Mutex
	protocol    *Protocol
	id          PeerId
	status      int
	localBytes  int // how many bytes we've agreed to store for this peer
	remoteBytes int // how many bytes peer is willing to store for us

	// cached values
	localUsedBytes  int
	remoteUsedBytes int
	shardsAccounted map[BlockShardId]bool // set of shards that we have accounted for in the cached remoteUsedBytes
}

func MakePeer(id PeerId, fluidBackup *FluidBackup, protocol *Protocol) *Peer {
	this := new(Peer)
	this.protocol = protocol
	this.id = id
	this.status = STATUS_ONLINE
	this.shardsAccounted = make(map[BlockShardId]bool)

	go func() {
		/* keep updating until eternity */
		for !fluidBackup.Stopping() {
			this.update()

			if Debug {
				time.Sleep(time.Duration(rand.Intn(3000))*time.Millisecond + 3*time.Second)
			} else {
				time.Sleep(time.Duration(rand.Intn(60000))*time.Millisecond + 30*time.Second)
			}
		}
	}()

	return this
}

/*
 * Our represented peer wants to propose agreement
 * with the local peer.
 */
func (this *Peer) proposeAgreement(localBytes int, remoteBytes int) bool {
	if this.protocol.proposeAgreement(this.id, localBytes, remoteBytes) {
		this.eventAgreement(localBytes, remoteBytes)
		return true
	}

	return false
}

func (this *Peer) eventAgreement(localBytes int, remoteBytes int) {
	Log.Debug.Printf("New agreement with %s (%d to %d)", this.id.String(), localBytes, remoteBytes)
	this.mu.Lock()
	this.localBytes += localBytes
	this.remoteBytes += remoteBytes
	this.mu.Unlock()
}

/*
 * Replicates a shard that the local peer wants to store on this peer.
 */
func (this *Peer) storeShard(shard *BlockShard) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	_, ok := this.shardsAccounted[shard.Id]
	if !ok {
		// this is bad, blockstore is trying to store a shard that hasn't been reserved yet?
		Log.Error.Printf("Peer handler %s received unaccounted shard %d!", this.id.String(), shard.Id)
		return false
	}

	return this.protocol.storeShard(this.id, int64(shard.Id), shard.Contents)
}

func (this *Peer) retrieveShard(shard *BlockShard) []byte {
	return this.protocol.retrieveShard(this.id, int64(shard.Id))
}

/*
 * Attempts to reserve a number of bytes for storage on this peer.
 * Returns true if the bytes have been reserved for use by caller, or false if reservation failed.
 *
 * Note that this is also used on startup to register reservations that were made earlier.
 */
func (this *Peer) reserveBytes(bytes int, shardId BlockShardId) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	_, ok := this.shardsAccounted[shardId]
	if ok {
		// this should never happen: we should only make a reservation once
		// to try and handle this, we (asynchronously) notify remote end that this shard
		//  should be removed from their storage; we fail the reservation until this deletion
		//  is completed
		// it is possible but even more unlikely that this is triggered on startup when we
		//  are registering past reservations; in that case this is still handled correctly
		//  since the caller will delete the reservation detail and re-replicate
		// TODO: tell peer to remove the shard
		return false
	}

	if this.remoteBytes-this.remoteUsedBytes >= bytes {
		this.remoteUsedBytes += bytes
		this.shardsAccounted[shardId] = true
		return true
	} else {
		return false
	}
}

func (this *Peer) getShardPath(label int64) string {
	// todo: make the store directory automatically
	return fmt.Sprintf("store/%s_%d.obj", this.id.String(), label)
}

/*
 * called on a representation
 * to say that the peer it represents is trying to
 * store data on our peer
 */
func (this *Peer) eventStoreShard(label int64, bytes []byte) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	// confirm the peer still has space on our storage to reserve
	if this.localBytes < this.localUsedBytes+len(bytes) {
		return false
	}

	// okay, store it in the file and update cached usage
	err := ioutil.WriteFile(this.getShardPath(label), bytes, 0644)

	if err != nil {
		Log.Warn.Printf("Failed to write peer shard (%s #%d): %s", this.id.String(), label, err.Error())
		return false
	}

	this.remoteUsedBytes += len(bytes)
	return true
}

func (this *Peer) eventRetrieveShard(label int64) []byte {
	this.mu.Lock()
	defer this.mu.Unlock()

	shardBytes, err := ioutil.ReadFile(this.getShardPath(label))

	if err != nil {
		Log.Warn.Printf("Failed to handle shard retrieval request (%s #%d): %s", this.id.String(), label, err.Error())
		return nil
	} else {
		return shardBytes
	}
}

func (this *Peer) isOnline() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.status == STATUS_ONLINE
}

/*
 * Call as often as reasonable.
 * Syncs this peer representation with the actual remote
 * peer.
 */
func (this *Peer) update() {
	online := this.protocol.ping(this.id)

	this.mu.Lock()
	if online && this.status == STATUS_OFFLINE {
		Log.Info.Printf("Peer %s came online", this.id.String())
		this.status = STATUS_ONLINE
	} else if !online && this.status == STATUS_ONLINE {
		Log.Info.Printf("Peer %s went offline", this.id.String())
		this.status = STATUS_OFFLINE
	}
	this.mu.Unlock()
}
