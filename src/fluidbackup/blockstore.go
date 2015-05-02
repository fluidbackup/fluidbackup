package fluidbackup

import "sync"
import "math/rand"
import "time"
import "bytes"

type BlockId int64
type BlockShardId int64

/*
 * BlockShard represents a slice of a file block.
 */
type BlockShard struct {
	Id BlockShardId
	Hash []byte
	Peer *Peer
	Available bool // whether the peer has confirmed receipt of the shard

	Parent *Block
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
	Id BlockId
	Hash []byte
	N int
	K int
	Shards []*BlockShard

	// source
	ParentFile string
	FileOffset int
}

type BlockStore struct {
	mu sync.Mutex
	peerList *PeerList
	blocks map[BlockId]*Block
	replicateN, replicateK int
}

func MakeBlockStore(peerList *PeerList) *BlockStore {
	this := new(BlockStore)
	this.peerList = peerList
	this.blocks = make(map[BlockId]*Block, 0)
	this.replicateN = DEFAULT_N
	this.replicateK = DEFAULT_K

	go func() {
		for {
			this.update()
			time.Sleep(time.Duration(50 * time.Millisecond))
		}
	}()

	return this
}

func (this *BlockStore) RegisterBlock(path string, offset int, contents []byte) *Block {
	this.mu.Lock()
	defer this.mu.Unlock()

	block := &Block{}
	block.Id = BlockId(rand.Int63())
	block.N = this.replicateN
	block.K = this.replicateK
	block.ParentFile = path
	block.FileOffset = offset
	block.Hash = hash(contents)

	shards := erasureEncode(contents, block.K, block.N)
	block.Shards = make([]*BlockShard, block.N)

	for shardIndex, shardBytes := range shards {
		block.Shards[shardIndex] = &BlockShard{
			Id: BlockShardId(rand.Int63()),
			Hash: hash(shardBytes),
			Peer: nil,
			Available: false,
			Contents: shardBytes,
			Parent: block,
			ShardIndex: shardIndex,
		}
	}

	this.blocks[block.Id] = block
	Log.Debug.Printf("Registered new block %d with %d shards", block.Id, len(block.Shards))
	return block
}

func (this *BlockStore) RecoverBlock(block *Block) []byte {
	this.mu.Lock()
	defer this.mu.Unlock()

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
