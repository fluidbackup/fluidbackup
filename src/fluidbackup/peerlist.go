package fluidbackup

import "sync"
import "math/rand"
import "time"
import "os"
import "fmt"
import "bufio"
import "strings"
import "sort"

type PeerRequest struct {
	Bytes int
	IgnorePeers map[PeerId]bool

	// a list of peers that we are already using
	// and how much use they are getting
	PeerUsage map[PeerId]int
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

	// a map of remote peers to their locally-assigned
	// trust scores, used for peer discovery.
	// See trust score section for documentation.
	peerTrustScores map[PeerId]int
	muTrust         sync.Mutex

	// Maintain a map of related peers
	// for each peerId
	relatedPeers map[PeerId][]PeerId

	// How many peers do we want this peer list to have?
	// the peer list will attempt to keep around this number
	// of peers in the network at all times.
	desiredNumPeers int
}

/*
 * Config
 */
const PeerListDefaultDesiredNumPeers int = 10

/*
 * Constructor
 */
func MakePeerList(fluidBackup *FluidBackup, protocol *Protocol) *PeerList {
	this := new(PeerList)
	this.fluidBackup = fluidBackup
	this.protocol = protocol
	this.peers = make(map[PeerId]*Peer)
	this.peerTrustScores = make(map[PeerId]int)
	this.relatedPeers = make(map[PeerId][]PeerId)
	this.desiredNumPeers = PeerListDefaultDesiredNumPeers

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
		peer = MakePeer(peerId, this.fluidBackup, this.protocol, this)
		this.peers[peerId] = peer
		// Add to trust map, initial score of 1.
		this.AddPeerToTrustStore(peerId)
	}
	return peer
}

/*
 * Attempts to identify an available peer.
 * If multiple, returns a trustworthy peer, such that the number
 * of bytes on that peer follows the trust distribution.
 * Caller requires [bytes] free bytes to use and doesn't want the peer to be in [ignorePeers] list.
 * [shard] is the BlockShard that will be replicated with this reservation (used for space accounting).
 * Returns nil if we aren't able to satisfy the request (in this case we should also launch task to get more space).
 */
func (this *PeerList) FindAvailablePeer(bytes int, peerUsage map[PeerId] int, ignorePeers map[PeerId]bool, shardId BlockShardId) *Peer {
	this.mu.Lock()
	defer this.mu.Unlock()

	availablePeers := this.availablePeersByTrust(peerUsage, ignorePeers)

	for _, peerId := range availablePeers {
		peer := this.peers[peerId]
		if peer.reserveBytes(bytes, shardId, false) {
			return peer
		}
	}

	// none of our peers could satisfy this request with existing agreements
	// update last request so that we will create new agreement and eventually satisfy (but don't overwrite requests with less constraints)
	if this.lastRequest == nil || len(this.lastRequest.IgnorePeers) > len(ignorePeers) {
		this.lastRequest = &PeerRequest{Bytes: bytes, PeerUsage: peerUsage, IgnorePeers: ignorePeers}
	}
	return nil
}

// a private sum helper
func sum(mapToSum map[PeerId]int) int {
	sum := 0
	for _, value := range mapToSum {
		sum += value
	}
	return sum
}

/*
 * Returns a list of peers ordered such that
 * the number of shards distributed on each peer
 * follows the trust distribution, and such that
 * the ordering respects how many more "shard slots"
 * are available on each peer (0 (lots) -> len (none))
 *
 * TODO: if trust distribution changes, current storage
 * distribution does not change. Maybe reconsider?
 */
func (this *PeerList) availablePeersByTrust(peerLoadDistribution map[PeerId]int, ignorePeers map[PeerId]bool) []PeerId {

	numShards := sum(peerLoadDistribution)
	trustSum := sum(this.peerTrustScores)

	if trustSum == 0 {
		return nil
	}

	// make a map of scores to sort by
	peerAvailability := make(map[PeerId]int)
	for peerId, load := range peerLoadDistribution {
		expectedNum := int(this.peerTrustScores[peerId] * numShards / trustSum)
		peerAvailability[peerId] = expectedNum - load
	}

	sliceOfPeerIds := make([]PeerId, 0, len(this.peers))
	for peerId, peer := range this.peers {
		// only consider online peers that shouldn't be ignored
		_, ignored := ignorePeers[peerId]
		if peer.isOnline() && !ignored {
			sliceOfPeerIds = append(sliceOfPeerIds, peerId)
		}
	}
	sort.Sort(ByScore{sliceOfPeerIds, peerAvailability})
	return sliceOfPeerIds
}

func (this *PeerList) update() {
	this.mu.Lock()

	if len(this.peers) < this.desiredNumPeers {
		difference := this.desiredNumPeers - len(this.peers)
		oldNum := len(this.peers)
		this.mu.Unlock()
		newNum := this.FindNewPeers(difference)
		this.mu.Lock()

		if newNum > oldNum {
			Log.Info.Printf("Found new peers, network peers from %d => %d.", oldNum, newNum)
		}
	}

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
	// pick best available peer
	this.mu.Lock()
	possiblePeers := make([]*Peer, 0)
	availablePeers := this.availablePeersByTrust(request.PeerUsage, request.IgnorePeers)
	for _, peerId := range availablePeers {
		peer := this.peers[peerId]
		if peer.isOnline() {
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

// called from protocol
func (this *PeerList) HandleStoreShard(peerId PeerId, label int64, bytes []byte) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	peer, ok := this.peers[peerId]
	if !ok {
		return false
	}

	return peer.eventStoreShard(label, bytes)
}

func (this *PeerList) HandleDeleteShard(peerId PeerId, label int64) {
	this.mu.Lock()
	defer this.mu.Unlock()

	peer, ok := this.peers[peerId]
	if !ok {
		return
	}

	peer.eventDeleteShard(label)
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

/* ================ *
 * Trust Management *
 * ================ */
// Maintains a trust store (map) of peers
// Scores from 0 (least trustworthy) to large ints
// consider breaking into separate module
// Trust is based on:
// - related peers (one direction) (map)
// - file storage agreement upholding
// (externally exposed)
// Trust is used for: picking where to store things

// private method
func (this *PeerList) ensurePeerInTrustStore(peerId PeerId) {
	_, ok := this.peerTrustScores[peerId]
	if !ok {
		// default initial trust score
		this.peerTrustScores[peerId] = 1
	}
}

// Return a list of peers sorted by trust
func (this *PeerList) peersSortedByTrust() []PeerId {
	sliceOfPeerIds := make([]PeerId, len(this.peerTrustScores))
	i := 0
	for peerId, _ := range this.peerTrustScores {
		sliceOfPeerIds[i] = peerId
		i += 1
	}
	sort.Sort(ByScore{sliceOfPeerIds, this.peerTrustScores})
	return sliceOfPeerIds
}

// Update trust score geometrically based on success
func (this *PeerList) updateTrustGeometrically(peerId PeerId, success bool) {
	this.muTrust.Lock()
	defer this.muTrust.Unlock()

	this.ensurePeerInTrustStore(peerId)
	if success {
		// do so geometrically
		this.peerTrustScores[peerId] *= 2
		// update related peers
		relatedPeers := this.relatedPeers[peerId]
		for _, relatedPeerId := range relatedPeers {
			this.peerTrustScores[relatedPeerId] = int(float64(this.peerTrustScores[relatedPeerId]) * 1.5)
		}
	} else {
		formerScore := this.peerTrustScores[peerId]
		this.peerTrustScores[peerId] = int(formerScore / 2)

		// update elated peers
		relatedPeers := this.relatedPeers[peerId]
		for _, relatedPeerId := range relatedPeers {
			this.peerTrustScores[relatedPeerId] = int(2 / 3 * this.peerTrustScores[relatedPeerId])
		}
	}
}

/*
 * Insert a new peer Id into our trust store
 */
func (this *PeerList) AddPeerToTrustStore(peerId PeerId) {
	this.ensurePeerInTrustStore(peerId)
}

/*
 * Update the trust score of a peer in response to a
 * good/bad retrieval
 */
func (this *PeerList) UpdateTrustPostRetrieval(peerId PeerId, success bool) {
	this.updateTrustGeometrically(peerId, success)
}

/*
 * Update the trust score of a peer in response to a
 * good/bad storage
 */
func (this *PeerList) UpdateTrustPostStorage(peerId PeerId, success bool) {
	this.updateTrustGeometrically(peerId, success)
}

/*
 * Update the trust score of a peer in response to a
 * verification attempt
 */
func (this *PeerList) UpdateTrustPostVerification(peerId PeerId, success bool) {
	this.updateTrustGeometrically(peerId, success)
}

/* ============== *
 * Peer Discovery *
 * ============== */
// How?
// Ask trusted peers for more peers.
// Need incentive for peer to share good peers
//    - incentive to share: want another trustworthy peer
//      to share with
//    - the trust rating of a peer is related to how good
//      the peers it shares are. Global trust ratings?
//    - trust ratings not global. But some peers may
//      choose to reveal their stored trust ratings

/*
 * Mechanism for sorting PeerIDs by Trust or an arbitrary Score
 */
type ByScore struct {
	// the peer Ids to sort
	peerIds []PeerId
	// the score to sort by
	peerScores map[PeerId]int
}

func (sorter ByScore) Len() int {
	return len(sorter.peerIds)
}
func (sorter ByScore) Swap(i, j int) {
	pIds := sorter.peerIds
	pIds[i], pIds[j] = pIds[j], pIds[i]
}
func (sorter ByScore) Less(i, j int) bool {
	peerIdA := sorter.peerIds[i]
	peerIdB := sorter.peerIds[j]
	return sorter.peerScores[peerIdA] < sorter.peerScores[peerIdB]
}

/*
 * public way to see how many peers we have.
 */
func (this *PeerList) NumPeers() int {
	return len(this.peers)
}

/*
 * Exposed way to find (num) new peers through our current
 * peer network.
 * Returns how total number of current peers after execution.
 */
func (this *PeerList) FindNewPeers(num int) int {
	this.findNewPeers(num)
	return len(this.peers)
}

/*
 * A convenience data holder for peer referral,
 * used in findNewPeers.
 */
type PeerReferral struct {
	sourcePeerId    PeerId
	referredPeerIds []PeerId
}

/*
 * Attempt to find (num) new peers through our current
 * peer network.
 * Returns how many peers were actually found in this pass.
 * This number can be more than the given, which is acceptable,
 * or less, which is less tolerable if there are peers available.
 */
func (this *PeerList) findNewPeers(num int) int {
	// prevent anyone else from modifying peers
	// and trust scores while we are working with them
	this.muTrust.Lock()

	// First, build a list of peers sorted by trust
	peersByTrust := this.peersSortedByTrust()

	// Go through the top trustedPeers
	var numNewPeers int
	var newPeerReferrals []PeerReferral
	for _, peerId := range peersByTrust {
		numAlreadyShared := len(this.relatedPeers[peerId])
		if numAlreadyShared > num {
			continue
		}
		// synchronously ask for more peers (for now)
		// (todo: ask asynchronously to improve performance)
		sharedPeerIds := this.peers[peerId].askForPeers(num)
		newPeerReferral := PeerReferral{peerId, sharedPeerIds}
		newPeerReferrals = append(newPeerReferrals, newPeerReferral)
		numNewPeers += len(sharedPeerIds) - numAlreadyShared
		if numNewPeers > num {
			// we should have about enough peers
			break
		}
	}

	this.muTrust.Unlock()

	// add new peers to our peerList
	trueNewPeerCount := 0
	for _, peerReferral := range newPeerReferrals {
		for _, peerId := range peerReferral.referredPeerIds {
			// ignore ourself
			if peerId == this.protocol.GetMe() {
				continue
			}
			// ignore ones that we already have
			if _, ok := this.peers[peerId]; ok {
				continue
			}
			// add to related peers
			relatedPeerList := this.relatedPeers[peerReferral.sourcePeerId]
			relatedPeerList = append(relatedPeerList, peerId)
			this.relatedPeers[peerReferral.sourcePeerId] = relatedPeerList

			// finally, actually discover the peer.
			this.DiscoveredPeer(peerId)
			trueNewPeerCount += 1
		}
	}

	return trueNewPeerCount
}

/*
 * Another peer asked us for shared peers.
 * Currently give the other peer the peers we trust the most.
 * ^ May be worth optimizing such that we don't immediately give new
 * peers good peers. todo
 */
func (this *PeerList) HandleShareNewPeers(peerId PeerId, num int) []PeerId {
	// First, build a list of peers sorted by trust
	peersByTrust := this.peersSortedByTrust()
	length := len(peersByTrust)
	if num < length {
		length = num
	}
	peerList := make([]PeerId, length)
	i := 0
	for _, peerId := range peersByTrust[:length] {
		peerList[i] = peerId
		i += 1
	}
	return peerList
}
