package surfstore

import (
	"io/fs"
	"io/ioutil"
	"os"
)

func ClientSync(client RPCClient) {
	var blockStoreAddr string
	client.GetBlockStoreAddr(&blockStoreAddr)

	localLastMap, _ := LoadMetaFromMetaFile(client.BaseDir)
	localCurrentMap := ComputeLocalCurrentMap(client)
	SetRemovedFileHashes(localCurrentMap, localLastMap)
	remoteMap := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remoteMap)
	allFileNames := GetallFileName(remoteMap, localLastMap, localCurrentMap)
	resync := false
	for _, filename := range allFileNames {
		remoteInfo, remoteExist := remoteMap[filename]
		localCurrentInfo := localCurrentMap[filename]
		if !remoteExist {
			f, err := os.ReadFile(ConcatPath(client.BaseDir, localCurrentInfo.Filename))
			if err != nil {
				continue
			}
			client.PutBlock(f, blockStoreAddr, client.BlockSize, localCurrentInfo.BlockHashList)
			fileMetaData := &FileMetaData{}
			fileMetaData.Filename = filename
			fileMetaData.Version = 1
			fileMetaData.BlockHashList = localCurrentInfo.BlockHashList
			var latestVersion int32
			updateErr := client.UpdateFile(fileMetaData, &latestVersion)
			if updateErr != nil {
				resync = true
			}
			localLastMap[filename] = fileMetaData
			continue
		}

		localLastInfo, localLastExist := localLastMap[filename]
		if !localLastExist || (localLastExist && remoteInfo.Version != localLastInfo.Version) {
			if localLastExist {
				localLastMap[filename].BlockHashList = remoteInfo.BlockHashList
				localLastMap[filename].Version = remoteInfo.Version
			} else {
				fileMetaData := &FileMetaData{}
				fileMetaData.Filename = filename
				fileMetaData.Version = remoteInfo.Version
				fileMetaData.BlockHashList = remoteInfo.BlockHashList
				localLastMap[filename] = fileMetaData
			}
			var removedRemoteHashes []string
			removedRemoteHashes = append(removedRemoteHashes, "0")
			if hashesIsEqual(remoteInfo.BlockHashList, removedRemoteHashes) {
				os.Remove(ConcatPath(client.BaseDir, filename))
			} else {
				f, _ := os.Create(ConcatPath(client.BaseDir, filename))
				blocks := client.GetBlock(remoteInfo.BlockHashList, blockStoreAddr)
				for i := 0; i < len(blocks); i++ {
					f.Write(blocks[i])
				}
			}
			continue
		}
		isEqual := hashesIsEqual(localCurrentInfo.BlockHashList, localLastInfo.BlockHashList)
		if localLastExist && !isEqual {
			f, err := os.ReadFile(ConcatPath(client.BaseDir, localCurrentInfo.Filename))
			if err != nil {
				continue
			}
			client.PutBlock(f, blockStoreAddr, client.BlockSize, localCurrentInfo.BlockHashList)
			fileMetaData := &FileMetaData{}
			fileMetaData.Filename = filename
			fileMetaData.Version = localLastInfo.Version + 1
			fileMetaData.BlockHashList = localCurrentInfo.BlockHashList
			var latestVersion int32
			updateErr := client.UpdateFile(fileMetaData, &latestVersion)
			if updateErr != nil {
				resync = true
				continue
			}
			localLastMap[filename] = fileMetaData
			continue
		}
	}
	WriteMetaFile(localLastMap, client.BaseDir)
	if resync {
		resync = false
		ClientSync(client)
	}
}

func hashesIsEqual(h1 []string, h2 []string) bool {
	length := 0
	if len(h1) > len(h2) {
		length = len(h2)
	} else {
		length = len(h1)
	}
	for i := 0; i < length; i++ {
		if h1[i] != h2[i] {
			return false
		}
	}
	return true
}

func GetallFileName(remoteMap map[string]*FileMetaData, localLastMap map[string]*FileMetaData, localCurrentMap map[string]*FileMetaData) []string {
	var filenames []string
	for filename := range localLastMap {
		filenames = append(filenames, filename)
	}
	for filename := range localCurrentMap {
		_, localCurrentExist := localLastMap[filename]
		if localCurrentExist {
			continue
		}
		filenames = append(filenames, filename)
	}
	for filename := range remoteMap {
		_, localLastExist := localLastMap[filename]
		_, localCurrentExist := localCurrentMap[filename]
		if localLastExist || localCurrentExist {
			continue
		}
		filenames = append(filenames, filename)
	}
	return filenames
}

func ComputeLocalCurrentMap(client RPCClient) map[string]*FileMetaData {
	fileInfoMap := make(map[string]*FileMetaData)
	fileInfo, _ := ioutil.ReadDir(client.BaseDir)
	for _, filepath := range fileInfo {
		if isValid(filepath) {
			file, _ := os.ReadFile(ConcatPath(client.BaseDir, filepath.Name()))
			var hashes []string
			for i := 0; i < len(file); i += client.BlockSize {
				ed := i + client.BlockSize
				if ed > len(file) {
					ed = len(file)
				}
				hashes = append(hashes, GetBlockHashString(file[i:ed]))
			}
			fileMetaData := &FileMetaData{}
			fileMetaData.Filename = filepath.Name()
			fileMetaData.Version = 0
			fileMetaData.BlockHashList = hashes
			fileInfoMap[filepath.Name()] = fileMetaData
		}
	}
	return fileInfoMap
}

func isValid(filepath fs.FileInfo) bool {
	filename := filepath.Name()
	if filename != "index.txt" && filename[0] != '.' && filepath.Mode().IsRegular() {
		return true
	} else {
		return false
	}
}

func SetRemovedFileHashes(tmp map[string]*FileMetaData, localLastMap map[string]*FileMetaData) {
	for filename, _ := range localLastMap {
		fileInfo, ok := tmp[filename]
		if ok {
			tmp[filename] = fileInfo
		} else {
			fileMetaData := &FileMetaData{}
			fileMetaData.Filename = filename
			fileMetaData.Version = 0
			var hashes []string
			hashes = append(hashes, "0")
			fileMetaData.BlockHashList = hashes
			tmp[filename] = fileMetaData
		}
	}
}
