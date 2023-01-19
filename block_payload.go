package bstream

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/streamingfast/atm"
	"github.com/streamingfast/dstore"
)

var GetBlockPayloadSetter BlockPayloadSetter

type BlockPayloadSetter func(block *Block, data []byte) (*Block, error)

type BlockPayload interface {
	// Len returns the number of bytes the payload has. It's quite possible that
	// calling this will "load" the payload to know its amount of bytes. If the implementation
	// knows the lenght without loading the payload, it will not be loaded, otherwise a call
	// to `Get` will be performed.
	//
	// If a negative value is returned, an `error` occurred while loading the data, calling
	// `Get()` will return it.
	//
	// Otherwise, a positive value is returned, could be 0.
	Len() int64
	Get() (data []byte, err error)
}

type MemoryBlockPayload struct {
	data []byte
}

func MemoryBlockPayloadSetter(block *Block, data []byte) (*Block, error) {
	block.Payload = &MemoryBlockPayload{
		data: data,
	}

	return block, nil
}

func (p *MemoryBlockPayload) Len() (byteCount int64) {
	return int64(len(p.data))
}

func (p *MemoryBlockPayload) Get() (data []byte, err error) {
	return p.data, err
}

var atmCache *atm.Cache
var store dstore.Store

func getCache() *atm.Cache {
	if atmCache == nil {
		panic("cache is not initialized")
	}
	return atmCache
}

func InitCache(storeUrl string, cachePath string, maxRecentEntryBytes int, maxEntryByAgeBytes int) {
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		err := os.Mkdir(cachePath, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}

	var err error

	s, err := dstore.NewDBinStore(storeUrl)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize store: %s: %s", storeUrl, err))
	}

	store = s

	atmCache, err = atm.NewInitializedCache(cachePath, maxRecentEntryBytes, maxEntryByAgeBytes, atm.NewFileIO())
	if err != nil {
		panic(fmt.Sprintf("failed to initialize cache: %s: %s", cachePath, err))
	}
}

type ATMCachedBlockPayload struct {
	blockId  string
	blockNum uint64
	dataSize int
}

func (p *ATMCachedBlockPayload) Len() (byteCount int64) {
	data, err := p.Get()
	if err != nil {
		return -1
	}

	return int64(len(data))
}

func (p *ATMCachedBlockPayload) Get() (data []byte, err error) {
	data, found, err := getCache().Read(p.blockId)
	if found && err != nil {
		return nil, err
	}

	if len(data) == 0 {
		zlog.Info("block data is empty. reading block from filesource", zap.String("block_id", p.blockId), zap.Uint64("block_num", p.blockNum))

		var fs *FileSource
		var block *Block

		handler := HandlerFunc(func(blk *Block, obj interface{}) error {
			if blk.Num() != p.blockNum || blk.ID() != p.blockId {
				return nil
			}

			block = blk
			fs.Shutdown(nil)
			return nil
		})

		fs = NewFileSource(store, p.blockNum, handler, zlog)
		fs.Run()

		if fs.Err() != nil {
			return nil, fs.Err()
		}

		return block.Payload.Get()
	}

	return
}

func ATMCachedPayloadSetter(block *Block, data []byte) (*Block, error) {
	_, err := getCache().Write(block.Id, block.Timestamp, time.Now(), data)
	if err != nil {
		return nil, err
	}

	block.Payload = &ATMCachedBlockPayload{
		blockId:  block.Id,
		blockNum: block.Number,
		dataSize: len(data),
	}

	return block, err
}
