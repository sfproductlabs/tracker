// Copyright 2020 Andrew Grosser (dioptre@gmail.com) and other SF Product Labs authors.
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/coreos/pkg/capnslog"
)

const (
	// MaxKeyLength is the max length of keys allowed
	MaxKeyLength uint64 = 1024
)

const (
	// don't change these, see the comments on ExpertConfig.
	defaultExecShards  uint64 = 16
	defaultLogDBShards uint64 = 16
)

type LogDBCallback func(busy bool)

type ExpertConfig struct {
	// ExecShards is the number of execution shards in the first stage of the
	// execution engine. Default value is 16.
	ExecShards uint64
	// LogDBShards is the number of LogDB shards. Default value is 16.
	LogDBShards uint64
}

type LogDBConfig struct {
	expert                             ExpertConfig
	KVKeepLogFileNum                   uint64
	KVMaxBackgroundCompactions         uint64
	KVMaxBackgroundFlushes             uint64
	KVLRUCacheSize                     uint64
	KVWriteBufferSize                  uint64
	KVMaxWriteBufferNumber             uint64
	KVLevel0FileNumCompactionTrigger   uint64
	KVLevel0SlowdownWritesTrigger      uint64
	KVLevel0StopWritesTrigger          uint64
	KVMaxBytesForLevelBase             uint64
	KVMaxBytesForLevelMultiplier       uint64
	KVTargetFileSizeBase               uint64
	KVTargetFileSizeMultiplier         uint64
	KVLevelCompactionDynamicLevelBytes uint64
	KVRecycleLogFileNum                uint64
	KVNumOfLevels                      uint64
	KVBlockSize                        uint64
}

var (
	plog = capnslog.NewPackageLogger("github.com/sfproductlabs/tracker/v3", "pebblekv")
)

const (
	maxLogFileSize = 1024 * 1024 * 128
)

type eventListener struct {
	kv *KV
}

func (l *eventListener) close() {
	// close(l.stopped)
	// l.stopper.Stop()
	plog.Infof("Close called")
}

func (l *eventListener) notify() {
	select {
	case <-l.kv.dbSet:
		if l.kv.callback != nil {
			m := l.kv.db.Metrics()
			busy := uint64(m.MemTable.Count) >= l.kv.config.KVMaxWriteBufferNumber
			l.kv.callback(busy)
		}
	default:
	}
	plog.Infof("Notify called")
}

func (l *eventListener) onFlushEnd(pebble.FlushInfo) {
	// select {
	// case <-l.stopped:
	// 	return
	// default:
	// }
	// l.stopper.RunWorker(func() {
	// 	l.notify()
	// })
	plog.Infof("Flush end called")
}

func (l *eventListener) onWALCreated(pebble.WALCreateInfo) {
	// select {
	// case <-l.stopped:
	// 	return
	// default:
	// }
	// l.stopper.RunWorker(func() {
	// 	l.notify()
	// })
	plog.Infof("Wal Created called")
}

type pebbleWriteBatch struct {
	wb *pebble.Batch
	db *pebble.DB
	wo *pebble.WriteOptions
}

func (w *pebbleWriteBatch) Destroy() {
	w.wb.Close()
}

func (w *pebbleWriteBatch) Put(key []byte, val []byte) {
	if err := w.wb.Set(key, val, w.wo); err != nil {
		panic(err)
	}
}

func (w *pebbleWriteBatch) Delete(key []byte) {
	if err := w.wb.Delete(key, w.wo); err != nil {
		panic(err)
	}
}

func (w *pebbleWriteBatch) Clear() {
	// TODO:
	// we can't reuse the batch now as pebble's Batch.Reset() is still buggy
	// see https://github.com/cockroachdb/pebble/pull/849 for details
	w.wb.Close()
	w.wb = w.db.NewBatch()
}

func (w *pebbleWriteBatch) Count() int {
	return int(w.wb.Count())
}

type pebbleLogger struct{}

var _ pebble.Logger = (*pebbleLogger)(nil)

// PebbleLogger is the logger used by pebble
var PebbleLogger pebbleLogger

func (pebbleLogger) Infof(format string, args ...interface{}) {
	pebble.DefaultLogger.Infof(format, args...)
}

func (pebbleLogger) Fatalf(format string, args ...interface{}) {
	pebble.DefaultLogger.Infof(format, args...)
	panic(fmt.Errorf(format, args...))
}

// NewKVStore returns a pebble based IKVStore instance.
func NewKVStore(config LogDBConfig, callback LogDBCallback, dir string, wal string) (*KV, error) {
	return openPebbleDB(config, callback, dir, wal)
}

// KV is a pebble based IKVStore type.
type KV struct {
	db       *pebble.DB
	dbSet    chan struct{}
	opts     *pebble.Options
	ro       *pebble.IterOptions
	wo       *pebble.WriteOptions
	event    *eventListener
	callback LogDBCallback
	config   LogDBConfig
}

var pebbleWarning sync.Once

func openPebbleDB(config LogDBConfig, callback LogDBCallback,
	dir string, walDir string) (*KV, error) {
	blockSize := int(config.KVBlockSize)
	writeBufferSize := int(config.KVWriteBufferSize)
	maxWriteBufferNumber := int(config.KVMaxWriteBufferNumber)
	l0FileNumCompactionTrigger := int(config.KVLevel0FileNumCompactionTrigger)
	l0StopWritesTrigger := int(config.KVLevel0StopWritesTrigger)
	maxBytesForLevelBase := int64(config.KVMaxBytesForLevelBase)
	targetFileSizeBase := int64(config.KVTargetFileSizeBase)
	cacheSize := int64(config.KVLRUCacheSize)
	levelSizeMultiplier := int64(config.KVTargetFileSizeMultiplier)
	numOfLevels := int64(config.KVNumOfLevels)
	lopts := make([]pebble.LevelOptions, 0)
	sz := targetFileSizeBase
	for l := int64(0); l < numOfLevels; l++ {
		opt := pebble.LevelOptions{
			Compression:    pebble.NoCompression,
			BlockSize:      blockSize,
			TargetFileSize: sz,
		}
		sz = sz * levelSizeMultiplier
		lopts = append(lopts, opt)
	}
	cache := pebble.NewCache(cacheSize)
	ro := &pebble.IterOptions{}
	wo := &pebble.WriteOptions{Sync: true}
	opts := &pebble.Options{
		Levels:                      lopts,
		MaxManifestFileSize:         maxLogFileSize,
		MemTableSize:                writeBufferSize,
		MemTableStopWritesThreshold: maxWriteBufferNumber,
		LBaseMaxBytes:               maxBytesForLevelBase,
		L0CompactionThreshold:       l0FileNumCompactionTrigger,
		L0StopWritesThreshold:       l0StopWritesTrigger,
		Cache:                       cache,
		//Default fs is Disk and that's fine for now
		//FS:                          vfs.NewPebbleFS(fs),
		Logger: PebbleLogger,
	}
	kv := &KV{
		ro:       ro,
		wo:       wo,
		opts:     opts,
		config:   config,
		callback: callback,
		dbSet:    make(chan struct{}),
	}
	el := &eventListener{
		kv: kv,
	}
	opts.EventListener = pebble.EventListener{
		WALCreated: el.onWALCreated,
		FlushEnd:   el.onFlushEnd,
	}

	if len(walDir) > 0 {
		if err := os.MkdirAll(walDir, os.ModePerm); err != nil {
			return nil, err
		}
		opts.WALDir = walDir
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	pdb, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	cache.Unref()
	kv.db = pdb
	kv.setEventListener(el)
	return kv, nil
}

func (r *KV) setEventListener(el *eventListener) {
	if r.db == nil || r.event != nil {
		panic("unexpected kv state")
	}
	r.event = el
	close(r.dbSet)
	// force a WALCreated event as the one issued when opening the DB didn't get
	// handled
	el.onWALCreated(pebble.WALCreateInfo{})
}

// Name returns the IKVStore type name.
func (r *KV) Name() string {
	return "pebble"
}

// Close closes the RDB object.
func (r *KV) Close() error {
	r.event.close()
	if r.db != nil {
		r.db.Close()
	}
	r.db = nil
	return nil
}

func iteratorIsValid(iter *pebble.Iterator) bool {
	v := iter.Valid()
	if err := iter.Error(); err != nil {
		panic(err)
	}
	return v
}

// IterateValue ...
func (r *KV) IterateValue(fk []byte, lk []byte, inc bool,
	op func(key []byte, data []byte) (bool, error)) error {
	iter := r.db.NewIter(r.ro)
	defer iter.Close()
	for iter.SeekGE(fk); iteratorIsValid(iter); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		if inc {
			if bytes.Compare(key, lk) > 0 {
				return nil
			}
		} else {
			if bytes.Compare(key, lk) >= 0 {
				return nil
			}
		}
		cont, err := op(key, val)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

// GetValue ...
func (r *KV) GetValue(key []byte, op func([]byte) error) (err error) {
	val, closer, err := r.db.Get(key)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	defer func() {
		if closer != nil {
			if cerr := closer.Close(); err == nil {
				err = cerr
			}
		}
	}()
	return op(val)
}

// SaveValue ...
func (r *KV) SaveValue(key []byte, value []byte) error {
	return r.db.Set(key, value, r.wo)
}

// DeleteValue ...
func (r *KV) DeleteValue(key []byte) error {
	return r.db.Delete(key, r.wo)
}

// GetWriteBatch ...
func (r *KV) GetWriteBatch() *pebbleWriteBatch {
	return &pebbleWriteBatch{wb: r.db.NewBatch(), db: r.db, wo: r.wo}
}

// CommitWriteBatch ...
func (r *KV) CommitWriteBatch(pwb *pebbleWriteBatch) error {
	return r.db.Apply(pwb.wb, r.wo)
}

// BulkRemoveEntries ...
func (r *KV) BulkRemoveEntries(fk []byte, lk []byte) error {
	wb := r.db.NewBatch()
	defer wb.Close()
	if err := wb.DeleteRange(fk, lk, r.wo); err != nil {
		return err
	}
	return r.db.Apply(wb, r.wo)
}

// CompactEntries ...
func (r *KV) CompactEntries(fk []byte, lk []byte) error {
	return r.db.Compact(fk, lk)
}

// FullCompaction ...
func (r *KV) FullCompaction() error {
	fk := make([]byte, MaxKeyLength)
	lk := make([]byte, MaxKeyLength)
	for i := uint64(0); i < MaxKeyLength; i++ {
		fk[i] = 0
		lk[i] = 0xFF
	}
	return r.db.Compact(fk, lk)
}
