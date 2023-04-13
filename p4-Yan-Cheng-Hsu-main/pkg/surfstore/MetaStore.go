package surfstore

import (
	context "context"
	"errors"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	l              sync.RWMutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.l.RLock()
	defer m.l.RUnlock()
	fileInfoMap := &FileInfoMap{}
	fileInfoMap.FileInfoMap = m.FileMetaMap
	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.l.RLock()
	defer m.l.RUnlock()
	serverFileMetaData, ok := m.FileMetaMap[fileMetaData.Filename]
	v := &Version{}
	if ok && fileMetaData.Version != serverFileMetaData.Version+1 {
		v.Version = serverFileMetaData.Version
		return v, errors.New("Version too old but file exists")
	} else {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		v.Version = fileMetaData.Version
		return v, nil
	}

}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	blockServerAddr := &BlockStoreAddr{}
	blockServerAddr.Addr = m.BlockStoreAddr
	return blockServerAddr, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    make(map[string]*FileMetaData),
		BlockStoreAddr: blockStoreAddr,
	}
}
