package fluidbackup

import "sync"
import "math/rand"
import "time"
import "bytes"
import "strings"
import "os"
import "fmt"
import "encoding/hex"
import "bufio"

type BlockId int64
type BlockShardId int64

/*
 * BlockShard represents a slice of a file block.
 */
type BlockShard struct {
	Id        BlockShardId
	Hash      []byte
	Peer      *Peer
	Available bool // whether the peer has confirmed receipt of the shard

	Parent     *Block
	ShardIndex int

	// temporary fields
	Contents []byte // cleared once the peer confirms receipt of the shard
}

/*
 * Blocks are the unit of distribution.
 * A block is sliced via erasure coding into a number of shards, each of which is
 *  stored onto a different peer. The block can be recovered by collecting K of the
 *  N shards.
 */
type Block struct {
	Id     BlockId
	Hash   []byte
	N      int
	K      int
	Shards []*BlockShard

	// source
	ParentFile FileId
	FileOffset int
}

type BlockStore struct {
	mu       sync.Mutex
	peerList *PeerList
	blocks   map[BlockId]*Block
	replicateN, replicateK int
}

func MakeBlockStore(fluidBackup *FluidBackup, peerList *PeerList) *BlockStore {
	this := new(BlockStore)
	this.peerList = peerList
	this.blocks = make(map[BlockId]*Block, 0)
	this.replicateN = DEFAULT_N
	this.replicateK = DEFAULT_K

	// perpetually ensure blocks are synced
	go func() {
		for !fluidBackup.Stopping() {
			this.update()
			time.Sleep(time.Duration(50 * time.Millisecond))
		}
	}()

	return this
}

func (this *BlockStore) RegisterBlock(fileId FileId, offset int, contents []byte) BlockId {
	this.mu.Lock()
	defer this.mu.Unlock()

	block := &Block{}
	block.Id = BlockId(rand.Int63())
	block.N = this.replicateN
	block.K = this.replicateK
	block.ParentFile = fileId
	block.FileOffset = offset
	block.Hash = hash(contents)

	shards := erasureEncode(contents, block.K, block.N)
	block.Shards = make([]*BlockShard, block.N)

	for shardIndex, shardBytes := range shards {
		block.Shards[shardIndex] = &BlockShard{
			Id:         BlockShardId(rand.Int63()),
			Hash:       hash(shardBytes),
			Peer:       nil,
			Available:  false,
			Contents:   shardBytes,
			Parent:     block,
			ShardIndex: shardIndex,
		}
	}

	this.blocks[block.Id] = block
	Log.Debug.Printf("Registered new block %d with %d shards", block.Id, len(block.Shards))
	return block.Id
}

func (this *BlockStore) RecoverBlock(blockId BlockId) []byte {
	this.mu.Lock()
	defer this.mu.Unlock()

	// verify block exists
	block := this.blocks[blockId]
	if block == nil {
		return nil
	}

	// recover the block
	Log.Debug.Printf("Begin recovery of block %d", block.Id)
	shardBytes := make([][]byte, 0, block.K)
	shardChunks := make([]int, 0, block.K)

	for shardIndex, shard := range block.Shards {
		if shard.Peer == nil || !shard.Available {
			Log.Debug.Printf("Skipping shard %d: no peer or not available", shard.Id)
			continue
		}

		Log.Debug.Printf("Attempting to retrieve shard %d from peer %s", shard.Id, shard.Peer.id.String())
		currBytes := shard.Peer.retrieveShard(shard)

		if currBytes == nil {
			Log.Warn.Printf("Failed to retrieve shard %d from peer %s (empty response)", shard.Id, shard.Peer.id.String())
			continue
		}

		if bytes.Equal(hash(currBytes), shard.Hash) {
			Log.Debug.Printf("Retrieved shard %d successfully (idx=%d, len=%d)", shard.Id, shardIndex, len(currBytes))
			shardBytes = append(shardBytes, currBytes)
			shardChunks = append(shardChunks, shardIndex)

			if len(shardBytes) >= block.K {
				break
			}
		} else {
			Log.Warn.Printf("Failed to retrieve shard %d from peer %s (hash check failed, len=%d)", shard.Id, shard.Peer.id.String(), len(currBytes))
			continue
		}
	}

	if len(shardBytes) < block.K {
		Log.Warn.Printf("Failed to retrieve block %d: only got %d shards", block.Id, len(shardBytes))
		return nil
	}

	blockBytes := erasureDecode(shardBytes, block.K, block.N, shardChunks)

	if !bytes.Equal(hash(blockBytes), block.Hash) {
		Log.Error.Printf("Failed to recover block %d: hash check failed even though we retrieved K shards", block.Id)
		return nil
	}

	Log.Debug.Printf("Successfully recovered block %d", block.Id)
	return blockBytes
}

func (this *BlockStore) update() {
	this.mu.Lock()
	defer this.mu.Unlock()

	// search for shards that don't have peers
	for _, block := range this.blocks {
		// first pass: find existing used peers
		ignorePeers := make(map[PeerId]bool)
		for _, shard := range block.Shards {
			if shard.Peer != nil {
				ignorePeers[shard.Peer.id] = true
			}
		}

		// second pass: actually find new peers
		for _, shard := range block.Shards {
			if shard.Peer == nil {
				availablePeer := this.peerList.FindAvailablePeer(len(shard.Contents), ignorePeers)

				if availablePeer == nil {
					// no available peer for this shard, other shards in this block won't have peers either
					break
				} else {
					shard.Peer = availablePeer
					ignorePeers[shard.Peer.id] = true
				}
			}
		}
	}

	// commit shard data to peers
	// we only commit once per update iteration to avoid hogging the lock?
	for _, block := range this.blocks {
		for _, shard := range block.Shards {
			if shard.Peer != nil && !shard.Available {
				Log.Debug.Printf("Committing shard %d to peer %s", shard.Id, shard.Peer.id.String())
				if shard.Peer.storeShard(shard) {
					shard.Available = true
					shard.Contents = nil
				}
				break
			}
		}
	}
}

/*
 * blockstore metadata can be written and read from the disk using Save/Load functions below.
 * The file format is a block on each line, consisting of string:
 *     [blockid]:[fileid]:[file_offset]:[N]:[K]:[hex(hash)]:[shard1],[shard2],...,[shardn],
 * Each shard looks like:
       [shardid].[peerid].[available].[hex(hash)]
 */

func (this *BlockStore) Save() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	Log.Info.Printf("Saving metadata to blockstore.dat (%d blocks)", len(this.blocks))
	fout, err := os.Create("blockstore.dat")
	if err != nil {
		Log.Warn.Printf("Failed to save metadata to blockstore.dat: %s", err.Error())
		return false
	}
	defer fout.Close()

	for _, block := range this.blocks {
		blockDump := fmt.Sprintf("%d:%s:%d:%d:%d:%s:", block.Id, block.ParentFile, block.FileOffset, block.N, block.K, hex.EncodeToString(block.Hash))
		for _, shard := range block.Shards {
			peerString := ""
			if shard.Peer != nil {
				peerString = shard.Peer.id.String()
			}
			blockDump += fmt.Sprintf("%d/%s/%d/%s", shard.Id, peerString, boolToInt(shard.Available), hex.EncodeToString(shard.Hash)) + ","
		}
		blockDump += "\n"
		fout.Write([]byte(blockDump))
	}

	return true
}

func (this *BlockStore) Load() bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	fin, err := os.Open("blockstore.dat")
	if err != nil {
		Log.Warn.Printf("Failed to read metadata from blockstore.dat: %s", err.Error())
		return false
	}
	defer fin.Close()

	scanner := bufio.NewScanner(fin)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ":", 7)

		if len(parts) != 7 {
			continue
		}

		block := &Block{}
		block.Id = BlockId(strToInt64(parts[0]))
		block.ParentFile = FileId(parts[1])
		block.FileOffset = strToInt(parts[2])
		block.N = strToInt(parts[3])
		block.K = strToInt(parts[4])
		block.Hash, _ = hex.DecodeString(parts[5])

		shardStrings := strings.Split(parts[6], ",")
		block.Shards = make([]*BlockShard, len(shardStrings) - 1) // last element of shardStrings is empty
		for i, shardString := range shardStrings {
			if i < len(block.Shards) {
				shardParts := strings.Split(shardString, "/")

				if len(shardParts) != 4 {
					Log.Warn.Printf("Failed to read metadata from blockstore.dat: invalid shard [%s]", shardString)
					return false
				}

				shard := &BlockShard{}
				shard.Id = BlockShardId(strToInt64(shardParts[0]))
				if shardParts[1] != "" {
					shard.Peer = this.peerList.DiscoveredPeer(strToPeerId(shardParts[1]))
				}
				shard.Available = strToInt(shardParts[2]) == 1
				shard.Hash, _ = hex.DecodeString(shardParts[3])

				shard.Parent = block
				shard.ShardIndex = i
				block.Shards[i] = shard
			}
		}

		this.blocks[block.Id] = block
	}

	Log.Info.Printf("Loaded %d blocks", len(this.blocks))
	return true
}
