package surfstore

import (
	context "context"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(hashes []string, blockStoreAddr string) [][]byte {
	conn, _ := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer conn.Close()
	defer cancel()
	var blocks [][]byte
	for _, hash := range hashes {
		blockHash := &BlockHash{}
		blockHash.Hash = hash
		block, err := c.GetBlock(ctx, blockHash)
		if err != nil {
			log.Panic(err)
		}
		blocks = append(blocks, block.BlockData)
	}
	return blocks
}

func (surfClient *RPCClient) PutBlock(block []byte, blockStoreAddr string, blockSize int, hashList []string) {
	conn, _ := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer conn.Close()
	defer cancel()

	blockHashes := &BlockHashes{}
	blockHashes.Hashes = hashList
	existedHashes, e := c.HasBlocks(ctx, blockHashes)
	if e != nil {
		log.Panic(e)
	}
	existed := make(map[string]bool)
	for _, hash := range existedHashes.Hashes {
		existed[hash] = true
	}
	hashindex := 0
	for i := 0; i < len(block); i = i + blockSize {
		_, ok := existed[hashList[hashindex]]
		if !ok {
			end := i + blockSize
			if end > len(block) {
				end = len(block)
			}
			b := &Block{}
			b.BlockData = block[i:end]
			b.BlockSize = int32(len(b.BlockData))
			_, err := c.PutBlock(ctx, b)
			if err != nil {
				log.Panic(err)
			}
		}
		hashindex += 1
	}
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, _ := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer conn.Close()
	defer cancel()
	hashesIn := &BlockHashes{}
	hashesIn.Hashes = blockHashesIn
	hashes, _ := c.HasBlocks(ctx, hashesIn)
	*blockHashesOut = hashes.Hashes
	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(remoteMap *map[string]*FileMetaData) error {
	conn, _ := grpc.Dial(surfClient.MetaStoreAddrs, grpc.WithInsecure())
	c := NewMetaStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	defer conn.Close()
	fileInfoMap, e := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	if e != nil {
		panic("Servers crashed.")
	}
	if e == nil && fileInfoMap.FileInfoMap != nil {
		*remoteMap = fileInfoMap.FileInfoMap
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	conn, _ := grpc.Dial(surfClient.MetaStoreAddrs, grpc.WithInsecure())
	c := NewMetaStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer conn.Close()
	defer cancel()
	v, e := c.UpdateFile(ctx, fileMetaData)
	if e != nil {
		panic("Servers crashed.")
	}
	*latestVersion = v.Version
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	conn, _ := grpc.Dial(surfClient.MetaStoreAddrs, grpc.WithInsecure())
	c := NewMetaStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer conn.Close()
	defer cancel()
	addr, e := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if e != nil {
		return e
	}
	*blockStoreAddr = addr.Addr
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: hostPort,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
