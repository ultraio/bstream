// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bstream

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

var currentOpenFiles int64

type FileSource struct {
	*shutter.Shutter

	oneBlockFileMode bool
	// blocksStore is where we access the blocks archives.
	blocksStore dstore.Store

	// secondaryBlocksStores is an optional list of blocksStores where we look for blocks archives that were not found, in order
	secondaryBlocksStores []dstore.Store

	// blockReaderFactory creates a new `BlockReader` from an `io.Reader` instance
	blockReaderFactory BlockReaderFactory

	startBlockNum uint64
	preprocFunc   PreprocessFunc
	// gates incoming blocks based on Gator type BEFORE pre-processing
	gator Gator

	// fileStream is a chan of blocks coming from blocks archives, ordered
	// and parallelly processed
	fileStream         chan *incomingBlocksFile
	oneBlockFileStream chan *incomingOneBlockFiles

	handler Handler
	// retryDelay determines the time between attempts to retry the
	// download of blocks archives (most of the time, waiting for the
	// blocks archive to be written by some other process in semi
	// real-time)
	retryDelay time.Duration

	notFoundCallback func(uint64)

	logger                  *zap.Logger
	preprocessorThreadCount int
}

type FileSourceOption = func(s *FileSource)

func FileSourceWithTimeThresholdGator(threshold time.Duration) FileSourceOption {
	return func(s *FileSource) {
		s.logger.Info("setting time gator", zap.Duration("threshold", threshold))
		s.gator = NewTimeThresholdGator(threshold)
	}
}

func FileSourceWithConcurrentPreprocess(threadCount int) FileSourceOption {
	return func(s *FileSource) {
		s.preprocessorThreadCount = threadCount
	}
}

// FileSourceWithSecondaryBlocksStores adds a list of dstore.Store that will be tried in order, in case the default store does not contain the expected blockfile
func FileSourceWithSecondaryBlocksStores(blocksStores []dstore.Store) FileSourceOption {
	return func(s *FileSource) {
		s.secondaryBlocksStores = blocksStores
	}
}

func FileSourceWithLogger(logger *zap.Logger) FileSourceOption {
	return func(s *FileSource) {
		s.logger = logger
	}
}

// NewFileSource will pipe potentially stream you 99 blocks before the given `startBlockNum`.
func NewFileSource(
	blocksStore dstore.Store,
	startBlockNum uint64,
	parallelDownloads int,
	preprocFunc PreprocessFunc,
	h Handler,
	options ...FileSourceOption,
) *FileSource {
	blockReaderFactory := GetBlockReaderFactory

	s := &FileSource{
		startBlockNum:           startBlockNum,
		blocksStore:             blocksStore,
		blockReaderFactory:      blockReaderFactory,
		fileStream:              make(chan *incomingBlocksFile, parallelDownloads),
		oneBlockFileStream:      make(chan *incomingOneBlockFiles, parallelDownloads),
		Shutter:                 shutter.New(),
		preprocFunc:             preprocFunc,
		retryDelay:              4 * time.Second,
		handler:                 h,
		logger:                  zlog,
		preprocessorThreadCount: 1,
	}

	blockStoreUrl := blocksStore.BaseURL()
	s.oneBlockFileMode = len(blockStoreUrl.Query()["oneblocks"]) > 0

	for _, option := range options {
		option(s)
	}

	return s
}

// SetNotFoundCallback sets a callback function to be triggered when
// a blocks file is not found. Useful for joining with unmerged blocks
func (s *FileSource) SetNotFoundCallback(f func(missingBlockNum uint64)) {
	s.notFoundCallback = f
}

func (s *FileSource) Run() {
	s.Shutdown(s.run())
}

func (s *FileSource) run() error {
	if s.oneBlockFileMode {
		return s.runOneBlockFile()
	}
	return s.runMergeFile()
}

func (s *FileSource) runMergeFile() error {

	go s.launchSink()

	const filesBlocksIncrement = 100 /// HARD-CODED CONFIG HERE!
	currentIndex := s.startBlockNum
	var delay time.Duration
	for {
		time.Sleep(delay)

		if s.IsTerminating() {
			s.logger.Info("blocks archive streaming was asked to stop")
			return nil
		}

		baseBlockNum := currentIndex - (currentIndex % filesBlocksIncrement)
		s.logger.Debug("file stream looking for", zap.Uint64("base_block_num", baseBlockNum))

		blocksStore := s.blocksStore // default
		baseFilename := fmt.Sprintf("%010d", baseBlockNum)
		exists, err := blocksStore.FileExists(context.Background(), baseFilename)
		if err != nil {
			return fmt.Errorf("reading file existence: %w", err)
		}

		if !exists && s.secondaryBlocksStores != nil {
			for _, bs := range s.secondaryBlocksStores {
				found, err := bs.FileExists(context.Background(), baseFilename)
				if err != nil {
					return fmt.Errorf("reading file existence: %w", err)
				}
				if found {
					exists = true
					blocksStore = bs
					break
				}
			}
		}

		if !exists {
			s.logger.Info("reading from blocks store: file does not (yet?) exist, retrying in", zap.String("filename", blocksStore.ObjectPath(baseFilename)), zap.String("base_filename", baseFilename), zap.Any("retry_delay", s.retryDelay), zap.Int("secondary_blocks_stores_count", len(s.secondaryBlocksStores)))
			delay = s.retryDelay

			if s.notFoundCallback != nil {
				s.logger.Info("asking merger for missing files", zap.Uint64("base_block_num", baseBlockNum))
				mergerBaseBlockNum := baseBlockNum
				if mergerBaseBlockNum < GetProtocolFirstStreamableBlock {
					mergerBaseBlockNum = GetProtocolFirstStreamableBlock
				}
				s.notFoundCallback(mergerBaseBlockNum)
			}
			continue
		}
		delay = 0 * time.Second

		newIncomingFile := &incomingBlocksFile{
			filename: baseFilename,
			//todo: this channel size should be 0 or configurable. This is a memory pit!
			//todo: ... there is not multithread after this point.
			blocks: make(chan *PreprocessedBlock, 2),
		}

		s.logger.Debug("downloading archive file", zap.String("filename", newIncomingFile.filename))
		select {
		case <-s.Terminating():
			return s.Err()
		case s.fileStream <- newIncomingFile:
			zlog.Debug("new incoming file", zap.String("file_name", newIncomingFile.filename))
		}

		go func() {
			s.logger.Debug("launching processing of file", zap.String("base_filename", baseFilename))
			if err := s.streamIncomingFile(newIncomingFile, blocksStore); err != nil {
				s.Shutdown(fmt.Errorf("processing of file %q failed: %w", baseFilename, err))
			}
		}()

		currentIndex += filesBlocksIncrement
	}
}

type retryableError struct{ error }

func (e retryableError) Error() string { return e.error.Error() }
func (e retryableError) Unwrap() error { return e.error }
func isRetryable(err error) bool       { _, ok := err.(retryableError); return ok }

func (s *FileSource) streamReader(blockReader BlockReader, prevLastBlockRead BlockRef, output chan *PreprocessedBlock) (err error) {
	var previousLastBlockPassed bool
	if prevLastBlockRead == nil {
		previousLastBlockPassed = true
	}

	done := make(chan interface{})
	preprocessed := make(chan chan *PreprocessedBlock, s.preprocessorThreadCount)

	go func() {
		defer close(done)
		defer close(output)

		for {
			select {
			case <-s.Terminating():
				return
			case ppChan, ok := <-preprocessed:
				if !ok {
					return
				}
				select {
				case <-s.Terminating():
					return
				case preprocessBlock := <-ppChan:
					select {
					case <-s.Terminating():
						return
					case output <- preprocessBlock:
					}
				}
			}
		}
	}()

	for {
		if s.IsTerminating() {
			return
		}

		var blk *Block
		blk, err = blockReader.Read()
		if err != nil && err != io.EOF {
			close(preprocessed)
			return err
		}

		if err == io.EOF && (blk == nil || blk.Num() == 0) {
			close(preprocessed)
			break
		}

		blockNum := blk.Num()
		if blockNum < s.startBlockNum {
			continue
		}

		if !previousLastBlockPassed {
			s.logger.Debug("skipping because this is not the first attempt and we have not seen prevLastBlockRead yet", zap.Stringer("block", blk), zap.Stringer("prev_last_block_read", prevLastBlockRead))
			if prevLastBlockRead.ID() == blk.ID() {
				previousLastBlockPassed = true
			}
			continue
		}

		if s.gator != nil && !s.gator.Pass(blk) {
			s.logger.Debug("gator not passed dropping block")
			continue
		}
		out := make(chan *PreprocessedBlock, 1)

		select {
		case <-s.Terminating():
			return
		case preprocessed <- out:
		}
		go s.preprocess(blk, out)
	}

	<-done
	return nil
}

func (s *FileSource) preprocess(block *Block, out chan *PreprocessedBlock) {
	var obj interface{}
	var err error
	if s.preprocFunc != nil {
		obj, err = s.preprocFunc(block)
		if err != nil {
			s.Shutdown(fmt.Errorf("preprocess block: %s: %w", block, err))
			return
		}
	}
	zlog.Debug("block pre processed", zap.Stringer("block_ref", block))
	select {
	case <-s.Terminating():
		return
	case out <- &PreprocessedBlock{Block: block, Obj: obj}:
	}
}

func (s *FileSource) streamIncomingFile(newIncomingFile *incomingBlocksFile, blocksStore dstore.Store) error {
	atomic.AddInt64(&currentOpenFiles, 1)
	s.logger.Debug("open files", zap.Int64("count", atomic.LoadInt64(&currentOpenFiles)), zap.String("filename", newIncomingFile.filename))
	defer atomic.AddInt64(&currentOpenFiles, -1)

	var skipBlocksBefore BlockRef

	reader, err := blocksStore.OpenObject(context.Background(), newIncomingFile.filename)
	if err != nil {
		return fmt.Errorf("fetching %s from block store: %w", newIncomingFile.filename, err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			s.logger.Error("unable to close reader", zap.Error(err))
		}
	}()

	blockReader, err := s.blockReaderFactory.New(reader)
	if err != nil {
		return fmt.Errorf("unable to create block reader: %w", err)
	}

	if err := s.streamReader(blockReader, skipBlocksBefore, newIncomingFile.blocks); err != nil {
		return fmt.Errorf("error processing incoming file: %w", err)
	}

	return nil
}

func (s *FileSource) launchSink() {
	for {
		select {
		case <-s.Terminating():
			zlog.Debug("terminating by launch sink")
			return
		case incomingFile := <-s.fileStream:
			s.logger.Debug("feeding from incoming file", zap.String("filename", incomingFile.filename))

			for preBlock := range incomingFile.blocks {
				if s.IsTerminating() {
					return
				}

				if err := s.handler.ProcessBlock(preBlock.Block, preBlock.Obj); err != nil {
					s.Shutdown(fmt.Errorf("process block failed: %w", err))
					return
				}
			}
		}
	}
}

func (s *FileSource) SetLogger(logger *zap.Logger) {
	s.logger = logger
}
