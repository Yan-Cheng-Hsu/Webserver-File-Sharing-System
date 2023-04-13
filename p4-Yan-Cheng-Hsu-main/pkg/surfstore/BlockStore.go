package surfstore

import (
	context "context"
	"errors"
	"sync"
)

var l sync.RWMutex

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	l.RLock()
	defer l.RUnlock()
	block, ok := bs.BlockMap[blockHash.Hash]
	if ok {
		return block, nil
	} else {
		return nil, errors.New("hash key not exists")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	l.Lock()
	defer l.Unlock()
	bs.BlockMap[GetBlockHashString(block.BlockData)] = block
	suc := &Success{}
	suc.Flag = true
	return suc, nil
}

func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	l.RLock()
	defer l.RUnlock()
	blockHashesOut := &BlockHashes{}
	var hashes []string
	for _, hash := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[hash]
		if !ok {
			continue
		} else {
			hashes = append(hashes, hash)
		}
	}
	blockHashesOut.Hashes = hashes
	return blockHashesOut, nil
}

var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: make(map[string]*Block),
	}
}
