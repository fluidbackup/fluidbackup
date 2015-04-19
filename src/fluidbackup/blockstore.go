package fluidbackup

import "crypto/sha1"
import "sync"

type Block struct {
	Hash []byte
	N int
	K int
	Peers []*PeerId

	// source
	ParentFile string
	FileOffset int

	// temporary fields
	Contents []byte // cleared whenever the block is distributed across all N peers
}

type BlockStore struct {
	mu sync.Mutex
	blocks []*Block
}

func MakeBlockStore() *BlockStore {
	this := new(BlockStore)
	this.blocks = make([]*Block, 0)
	return this
}

func (this *BlockStore) RegisterBlock(path string, offset int, contents []byte) *Block {
	this.mu.Lock()
	defer this.mu.Unlock()

	block := &Block{}
	block.N = DEFAULT_N
	block.K = DEFAULT_K
	block.Peers = make([]*PeerId, block.N)
	block.ParentFile = path
	block.FileOffset = offset
	block.Contents = contents
	block.Hash = sha1.New().Sum(contents)

	this.blocks = append(this.blocks, block)
	return block
}
