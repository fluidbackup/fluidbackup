package fluidbackup

import "crypto/sha1"
import "sync"
import "math/rand"

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
}

func MakeBlockStore(peerList *PeerList) *BlockStore {
	this := new(BlockStore)
	this.peerList = peerList
	this.blocks = make(map[BlockId]*Block, 0)

	go func() {
		this.update()
	}()

	return this
}

func (this *BlockStore) RegisterBlock(path string, offset int, contents []byte) *Block {
	this.mu.Lock()
	defer this.mu.Unlock()

	block := &Block{}
	block.Id = BlockId(rand.Int63())
	block.N = DEFAULT_N
	block.K = DEFAULT_K
	block.ParentFile = path
	block.FileOffset = offset
	block.Hash = sha1.New().Sum(contents)

	shards := erasureEncode(contents, block.K, block.N)
	block.Shards = make([]*BlockShard, block.N)

	for shardIndex, shardBytes := range shards {
		block.Shards[shardIndex] = &BlockShard{
			Id: BlockShardId(rand.Int63()),
			Hash: sha1.New().Sum(shardBytes),
			Peer: nil,
			Available: false,
			Contents: shardBytes,
			Parent: block,
			ShardIndex: shardIndex,
		}
	}

	this.blocks[block.Id] = block
	return block
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
				if shard.Peer.storeShard(shard) {
					shard.Available = true
					shard.Contents = nil
				}
				break
			}
		}
	}
}
