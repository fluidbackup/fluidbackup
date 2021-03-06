package fluidbackup

import "sync"
import "fmt"
import "io/ioutil"
import "time"
import "math/rand"
import "strings"
import "os"

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
	fluidBackup *FluidBackup
	peerList    *PeerList
	id          PeerId
	status      int
	localBytes  int // how many bytes we've agreed to store for this peer
	remoteBytes int // how many bytes peer is willing to store for us

	// cached values
	localUsedBytes  int
	remoteUsedBytes int
	lastVerifyTime  time.Time // last time we performed a shard storage verification
	lastVerifySuccessTime time.Time
	verifyFailCount int // count of successive verification failures

	// set of shards that we have accounted for in the cached remoteUsedBytes
	// false if not replicated yet, true otherwise
	shardsAccounted map[BlockShardId]bool
}

func MakePeer(id PeerId, fluidBackup *FluidBackup, protocol *Protocol, peerList *PeerList) *Peer {
	this := new(Peer)
	this.fluidBackup = fluidBackup
	this.protocol = protocol
	this.id = id
	this.status = STATUS_ONLINE
	// save peerList for operations on our local peer in
	// response to simulations
	this.peerList = peerList
	this.shardsAccounted = make(map[BlockShardId]bool)
	this.accountLocalUsedBytes()

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
 * Our local peer wants to propose agreement
 * with the represented remote peer.
 * currently ONE Agreement per shard (TODO: change?)
 */
func (this *Peer) proposeAgreement(localBytes int, remoteBytes int) bool {
	if this.protocol.proposeAgreement(this.id, localBytes, remoteBytes) {
		this.eventAgreement(localBytes, remoteBytes)
		return true
	}

	return false
}

func (this *Peer) eventAgreement(localBytes int, remoteBytes int) {
	this.mu.Lock()
	this.localBytes += localBytes
	this.remoteBytes += remoteBytes
	Log.Debug.Printf("New agreement with %s (%d to %d; total %d/%d to %d/%d)", this.id.String(), localBytes, remoteBytes, this.localUsedBytes, this.localBytes, this.remoteUsedBytes, this.remoteBytes)
	this.mu.Unlock()
}

/*
 * Recomputes the number of bytes we are storing for this peer by searching filesystem.
 * Assumes caller has the lock.
 */
func (this *Peer) accountLocalUsedBytes() {
	oldUsedBytes := this.localUsedBytes
	this.localUsedBytes = 0
	files, _ := ioutil.ReadDir("store/")
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".obj") && strings.HasSuffix(f.Name(), this.id.String()+"_") {
			fi, err := os.Stat("store/" + f.Name())
			if err == nil {
				this.localUsedBytes += int(fi.Size())
			}
		}
	}

	Log.Debug.Printf("Re-accounted stored bytes from %d to %d", oldUsedBytes, this.localUsedBytes)
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

	this.shardsAccounted[shard.Id] = true
	result := this.protocol.storeShard(this.id, int64(shard.Id), shard.Contents)

	if result == 0 {
		return true
	} else if result == -2 {
		// peer actively refused the shard!
		// probably the agreement is not synchronized or peer terminated agreement
		go this.terminateAgreement()
	}

	return false
}

func (this *Peer) deleteShard(shardId BlockShardId) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	replicated, ok := this.shardsAccounted[shardId]
	if !ok || !replicated {
		// this is bad, blockstore is trying to delete a shard that hasn't been replicated yet?
		Log.Error.Printf("Peer handler %s received deletion request for unaccounted shard %d!", this.id.String(), shardId)
		return false
	}

	shard := this.fluidBackup.blockStore.GetShard(shardId)
	if shard == nil {
		Log.Error.Printf("Peer handler %s received deletion request for non-existent shard %d!", this.id.String(), shardId)
		return false
	}

	delete(this.shardsAccounted, shardId)
	this.protocol.deleteShard(this.id, int64(shardId))
	this.remoteUsedBytes -= shard.Length
	return true
}

func (this *Peer) retrieveShard(shardId BlockShardId) ([]byte, bool) {
	return this.protocol.retrieveShard(this.id, int64(shardId))
}

/*
 * Attempts to reserve a number of bytes for storage on this peer.
 * Returns true if the bytes have been reserved for use by caller, or false if reservation failed.
 *
 * Note that this is also used on startup to register reservations that were made earlier.
 */
func (this *Peer) reserveBytes(bytes int, shardId BlockShardId, alreadyReplicated bool) bool {
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
		go this.deleteShard(shardId)
		return false
	}

	if this.remoteBytes-this.remoteUsedBytes >= bytes {
		this.remoteUsedBytes += bytes
		this.shardsAccounted[shardId] = alreadyReplicated
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

	Log.Debug.Printf("Stored shard %d for peer %s (len=%d)", label, this.id.String(), len(bytes))
	this.remoteUsedBytes += len(bytes)
	return true
}

func (this *Peer) eventDeleteShard(label int64) {
	this.mu.Lock()
	defer this.mu.Unlock()
	os.Remove(this.getShardPath(label))
	this.accountLocalUsedBytes()
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
	// ping the peer
	// we do this outside the lock to avoid communication latency in the lock
	online := this.protocol.ping(this.id)

	this.mu.Lock()
	if online && this.status == STATUS_OFFLINE {
		Log.Info.Printf("Peer %s came online", this.id.String())
		this.status = STATUS_ONLINE
	} else if !online && this.status == STATUS_ONLINE {
		Log.Info.Printf("Peer %s went offline", this.id.String())
		this.status = STATUS_OFFLINE
	}

	verificationDelay := 300 * time.Second
	if Debug {
		verificationDelay = time.Second
	}

	var randomShard *BlockShardId
	if time.Now().After(this.lastVerifyTime.Add(verificationDelay)) {
		this.lastVerifyTime = time.Now()

		// pick a random shard to verify
		availableShards := make([]BlockShardId, 0)
		for shardId, available := range this.shardsAccounted {
			if available {
				availableShards = append(availableShards, shardId)
			}
		}

		if len(availableShards) > 0 {
			randomShard = &availableShards[rand.Intn(len(availableShards))]
		}
	}
	this.mu.Unlock()

	if randomShard != nil {
		Log.Debug.Printf("Verifying shard %d on peer %s", *randomShard, this.id.String())
		bytes, success := this.retrieveShard(*randomShard)

		if !success || !this.fluidBackup.blockStore.VerifyShard(this, *randomShard, bytes) {
			// either we failed to communicate with the peer (if !success),
			// or the peer sent us corrupted shard data (if success)
			failReason := "invalid shard data"
			if !success {
				failReason = "peer communication failed"
			}
			Log.Info.Printf("Failed verification of shard %d on peer %s: %s", *randomShard, this.id.String(), failReason)

			this.mu.Lock()
			if success {
				// shard is invalid, delete from remote end
				// block store will re-assign it already from the verifyshard call, so don't need to do anything else about that
				delete(this.shardsAccounted, *randomShard)
				go this.deleteShard(*randomShard)
			}

			// we also check if this peer is failed per our one-day policy, in which case we would want to clear all accounted shards
			this.verifyFailCount++
			if this.verifyFailCount > 5 && time.Now().After(this.lastVerifySuccessTime.Add(time.Second * VERIFY_FAIL_WAIT_INTERVAL)) {
				go this.terminateAgreement()
			}
			this.mu.Unlock()

			// Decrease trust
			this.peerList.UpdateTrustPostVerification(this.id, false)
		} else {
			this.peerList.UpdateTrustPostVerification(this.id, true)

			this.mu.Lock()
			this.verifyFailCount = 0
			this.lastVerifySuccessTime = time.Now()
			this.mu.Unlock()
		}
	}
}

func (this *Peer) terminateAgreement() {
	this.mu.Lock()
	Log.Info.Printf("Terminating agreement with peer %s", this.id.String())
	this.peerList.UpdateTrustPostVerification(this.id, false)

	for shardId := range this.shardsAccounted {
		this.fluidBackup.blockStore.VerifyShard(this, shardId, nil)
		delete(this.shardsAccounted, shardId)
	}

	this.localBytes = 0
	this.localUsedBytes = 0
	this.remoteBytes = 0
	this.remoteUsedBytes = 0

	// clear this peer's files from filesystem
	files, _ := ioutil.ReadDir("store/")
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".obj") && strings.HasSuffix(f.Name(), this.id.String()+"_") {
			os.Remove("store/" + f.Name())
		}
	}
	this.mu.Unlock()
}

/* ============== *
 * Peer Discovery *
 * ============== */

// Ask this remote peer for the
// specified number of peers
func (this *Peer) askForPeers(num int) []PeerId {
	sharedPeerIds := this.protocol.askForPeers(this.id, num)
	// Insert peer handler here if necesssay.
	return sharedPeerIds
}
