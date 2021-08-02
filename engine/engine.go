// Copyright 2021-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"fmt"
	"github.com/dgryski/go-farm"
	"github.com/ngaut/unistore/engine/cache"
	"github.com/ngaut/unistore/engine/compaction"
	"github.com/ngaut/unistore/engine/epoch"
	"github.com/ngaut/unistore/engine/table"
	"github.com/ngaut/unistore/engine/table/memtable"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/s3util"
	"github.com/ngaut/unistore/scheduler"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")

	ErrShardNotFound         = errors.New("shard not found")
	ErrShardNotMatch         = errors.New("shard not match")
	ErrPreSplitWrongStage    = errors.New("pre-split wrong stage")
	ErrSplitFilesWrongStage  = errors.New("split-files wrong stage")
	ErrFinishSplitWrongStage = errors.New("finish-split wrong stage")
)

type closers struct {
	compactors      *y.Closer
	resourceManager *y.Closer
	memtable        *y.Closer
	s3Client        *y.Closer
}

type Engine struct {
	opt           Options
	numCFs        int
	dirLock       *directoryLockGuard
	shardMap      sync.Map
	blkCache      *cache.Cache
	resourceMgr   *epoch.ResourceManager
	closers       closers
	flushCh       chan *flushTask
	flushResultCh chan *flushResultTask
	mangedSafeTS  uint64
	idAlloc       IDAllocator
	compClient    *compaction.Client
	s3c           *s3util.S3Client
	closed        uint32

	metaChangeListener MetaChangeListener
}

const (
	lockFile = "LOCK"
)

func OpenEngine(opt Options) (en *Engine, err error) {
	log.Info("Open Engine")
	err = checkOptions(&opt)
	if err != nil {
		return nil, err
	}
	var dirLockGuard *directoryLockGuard
	dirLockGuard, err = acquireDirectoryLock(opt.Dir, lockFile)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = dirLockGuard.release()
		}
	}()

	blkCache, err := createCache(opt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create block cache")
	}
	en = &Engine{
		opt:                opt,
		numCFs:             len(opt.CFs),
		dirLock:            dirLockGuard,
		blkCache:           blkCache,
		flushCh:            make(chan *flushTask, opt.NumMemtables),
		flushResultCh:      make(chan *flushResultTask, opt.NumMemtables),
		metaChangeListener: opt.MetaChangeListener,
	}
	en.idAlloc = opt.IDAllocator
	en.closers.resourceManager = y.NewCloser(0)
	en.resourceMgr = epoch.NewResourceManager(en.closers.resourceManager)
	en.closers.s3Client = y.NewCloser(0)
	if opt.S3Options.KeyID != "" {
		en.s3c = s3util.NewS3Client(en.closers.s3Client, opt.Dir, opt.InstanceID, opt.S3Options)
	}
	en.compClient = compaction.NewClient(opt.RemoteCompactionAddr, en.s3c)
	shardMetas, err := readMetas(opt.MetaReader)
	if err != nil {
		return nil, err
	}
	if err = en.loadShards(shardMetas); err != nil {
		return nil, errors.AddStack(err)
	}
	en.closers.memtable = y.NewCloser(2)
	go en.runFlushMemTable(en.closers.memtable)
	go en.runFlushResult(en.closers.memtable)
	if !en.opt.DoNotCompact {
		en.closers.compactors = y.NewCloser(1)
		go en.runCompactionLoop(en.closers.compactors)
	}
	return en, nil
}

func checkOptions(opt *Options) error {
	path := opt.Dir
	dirExists, err := exists(path)
	if err != nil {
		return y.Wrapf(err, "Invalid Dir: %q", path)
	}
	if !dirExists {
		// Try to create the directory
		err = os.Mkdir(path, 0700)
		if err != nil {
			return y.Wrapf(err, "Error Creating Dir: %q", path)
		}
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func createCache(opt Options) (blkCache *cache.Cache, err error) {
	if opt.MaxBlockCacheSize != 0 {
		blkCache, err = cache.NewCache(&cache.Config{
			// The expected keys is MaxCacheSize / BlockSize, then x10 as documentation suggests.
			NumCounters: opt.MaxBlockCacheSize / int64(opt.TableBuilderOptions.BlockSize) * 10,
			MaxCost:     opt.MaxBlockCacheSize,
			BufferItems: 64,
			OnEvict:     sstable.OnEvict,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to create block cache")
		}
	}
	return
}

func (en *Engine) DebugHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Time %s\n", time.Now().Format(time.RFC3339Nano))
		fmt.Fprintf(w, "FlushCh %s\n", formatInt(len(en.flushCh)))
		MemTables := 0
		MemTablesSize := 0
		L0Tables := 0
		L0TablesSize := 0
		CFs := 0
		LevelHandlers := 0
		CFsSize := 0
		LNTables := 0
		LNTablesSize := 0
		type shardStat struct {
			key           uint64
			ShardSize     int
			MemTablesSize int
			L0TablesSize  int
			CFsSize       int
			CFSize        [3]int
		}
		list := []shardStat{}
		en.shardMap.Range(func(key, value interface{}) bool {
			k := key.(uint64)
			shard := value.(*Shard)
			memTables := shard.loadMemTables()
			l0Tables := shard.loadL0Tables()
			MemTables += len(memTables.tables)
			ShardMemTablesSize := 0
			if shard.isSplitting() {
				MemTables += len(shard.splittingMemTbls)
				for i := 0; i < len(shard.splittingMemTbls); i++ {
					memTbl := shard.loadSplittingMemTable(i)
					ShardMemTablesSize += int(memTbl.Size())
				}
			}
			for _, t := range memTables.tables {
				ShardMemTablesSize += int(t.Size())
			}
			MemTablesSize += ShardMemTablesSize
			L0Tables += len(l0Tables.tables)
			ShardL0TablesSize := 0
			for _, t := range l0Tables.tables {
				ShardL0TablesSize += int(t.Size())
			}
			L0TablesSize += ShardL0TablesSize
			CFs += len(shard.cfs)
			ShardCFsSize := 0
			CFSize := [3]int{}
			for i, cf := range shard.cfs {
				LevelHandlers += len(cf.levels)
				for l := range cf.levels {
					level := cf.getLevelHandler(l + 1)
					CFSize[i] += int(level.totalSize)
					LNTables += len(level.tables)
					for _, t := range level.tables {
						LNTablesSize += int(t.Size())
					}
				}
				ShardCFsSize += CFSize[i]
			}
			CFsSize += ShardCFsSize
			stat := shardStat{
				key:           k,
				ShardSize:     ShardMemTablesSize + ShardL0TablesSize + ShardCFsSize,
				MemTablesSize: ShardMemTablesSize,
				L0TablesSize:  ShardL0TablesSize,
				CFsSize:       ShardCFsSize,
				CFSize:        CFSize,
			}
			list = append(list, stat)
			return true
		})
		fmt.Fprintf(w, "MemTables %s, MemTablesSize %s\n", formatInt(MemTables), formatInt(MemTablesSize))
		fmt.Fprintf(w, "L0Tables %s, L0TablesSize %s\n", formatInt(L0Tables), formatInt(L0TablesSize))
		fmt.Fprintf(w, "CFs %s, LevelHandlers %s, LNTables %s, CFsSize %s, LNTablesSize %s\n",
			formatInt(CFs),
			formatInt(LevelHandlers),
			formatInt(LNTables),
			formatInt(CFsSize),
			formatInt(LNTablesSize),
		)
		fmt.Fprintf(w, "Size %s\n", formatInt(MemTablesSize+L0TablesSize+CFsSize))
		fmt.Fprintf(w, "ShardMap %s\n", formatInt(len(list)))
		sort.Slice(list, func(i, j int) bool {
			return list[i].ShardSize > list[j].ShardSize
		})
		for _, shardStat := range list {
			key := shardStat.key
			if value, ok := en.shardMap.Load(key); ok {
				shard := value.(*Shard)
				memTables := shard.loadMemTables()
				l0Tables := shard.loadL0Tables()
				var splittings int
				if shard.isSplitting() {
					splittings = len(shard.splittingMemTbls)
				}
				if r.FormValue("detail") == "" {
					fmt.Fprintf(w, "\tShard\t% 13d:%d,\tSize % 13s,\tMem % 13s(%d),\tL0 % 13s(%d),\tCF0 % 13s,\tCF1 % 13s,\tMaxMemTblSize % 13s,\tStage % 20s, Passive %v\n\n",
						key,
						shard.Ver,
						formatInt(shardStat.ShardSize),
						formatInt(shardStat.MemTablesSize),
						len(memTables.tables)+splittings,
						formatInt(shardStat.L0TablesSize),
						len(l0Tables.tables),
						formatInt(shardStat.CFSize[0]),
						formatInt(shardStat.CFSize[1]),
						formatInt(int(shard.getMaxMemTableSize())),
						enginepb.SplitStage_name[shard.splitStage],
						shard.IsPassive(),
					)
					continue
				}
				fmt.Fprintf(w, "\tShard %d:%d, Size %s, Stage %s, Passive %v\n",
					key,
					shard.Ver,
					formatInt(shardStat.ShardSize),
					enginepb.SplitStage_name[shard.splitStage],
					shard.IsPassive(),
				)
				fmt.Fprintf(w, "\t\tMemTables %d, Size %s\n", len(memTables.tables)+splittings, formatInt(shardStat.MemTablesSize))
				if shard.isSplitting() {
					for i := 0; i < len(shard.splittingMemTbls); i++ {
						memTbl := shard.loadSplittingMemTable(i)
						if !memTbl.Empty() {
							fmt.Fprintf(w, "\t\t\tSplitting MemTable %d, Size %s\n", i, formatInt(int(memTbl.Size())))
						}
					}
				}
				for i, t := range memTables.tables {
					if !t.Empty() {
						fmt.Fprintf(w, "\t\t\tMemTable %d, Size %s\n", splittings+i, formatInt(int(t.Size())))
					}
				}
				fmt.Fprintf(w, "\t\tL0Tables %d,  Size %s\n", len(l0Tables.tables), formatInt(shardStat.L0TablesSize))
				for i, t := range l0Tables.tables {
					fmt.Fprintf(w, "\t\t\tL0Table %d, fid %d, size %s \n", i, t.ID(), formatInt(int(t.Size())))
				}
				fmt.Fprintf(w, "\t\tCFs Size %s\n", formatInt(shardStat.CFsSize))
				if shardStat.CFsSize > 0 {
					for i, cf := range shard.cfs {
						fmt.Fprintf(w, "\t\t\tCF %d, Size %s\n", i, formatInt(shardStat.CFSize[i]))
						if shardStat.CFSize[i] > 0 {
							for l := range cf.levels {
								level := cf.getLevelHandler(l + 1)
								fmt.Fprintf(w, "\t\t\t\tlevel %d, tables %s, totalSize %s \n",
									level.level,
									formatInt(len(level.tables)),
									formatInt(int(level.totalSize)),
								)
							}
						}

					}
				}
			}
		}
	}
}

func formatInt(n int) string {
	str := fmt.Sprintf("%d", n)
	length := len(str)
	if length <= 3 {
		return str
	}
	separators := (length - 1) / 3
	buf := make([]byte, length+separators)
	for i := 0; i < separators; i++ {
		buf[len(buf)-(i+1)*4] = ','
		copy(buf[len(buf)-(i+1)*4+1:], str[length-(i+1)*3:length-i*3])
	}
	copy(buf, str[:length-separators*3])
	return string(buf)
}

func (en *Engine) loadShards(shardMetas map[uint64]*ShardMeta) error {
	sche := scheduler.NewScheduler(en.opt.RecoveryConcurrency)
	var parents = make(map[uint64]struct{})
	parentsBT := scheduler.NewBatchTasks()
	bt := scheduler.NewBatchTasks()
	for _, v := range shardMetas {
		mShard := v
		parent := mShard.parent
		if parent != nil {
			if _, ok := parents[parent.ID]; !ok {
				parents[parent.ID] = struct{}{}
				parentsBT.AppendTask(func() error {
					parentShard, err := en.loadShard(parent)
					if err != nil {
						return errors.AddStack(err)
					}
					if en.opt.RecoverHandler != nil {
						err = en.opt.RecoverHandler.Recover(en, parentShard, parent)
						if err != nil {
							return errors.AddStack(err)
						}
					}
					return nil
				})
			}
		}
		bt.AppendTask(func() error {
			shard, err := en.loadShard(mShard)
			if err != nil {
				return err
			}
			if en.opt.RecoverHandler != nil {
				err = en.opt.RecoverHandler.Recover(en, shard, mShard)
				if err != nil {
					return errors.AddStack(err)
				}
			}
			return nil
		})
	}
	if err := sche.BatchSchedule(parentsBT); err != nil {
		return err
	}
	return sche.BatchSchedule(bt)
}

func (en *Engine) loadShard(shardInfo *ShardMeta) (*Shard, error) {
	if oldVal, ok := en.shardMap.Load(shardInfo.ID); ok && oldVal != nil {
		shard := oldVal.(*Shard)
		if shard.Ver == shardInfo.Ver {
			return shard, nil
		}
		l0s := shard.loadL0Tables()
		for _, l0 := range l0s.tables {
			l0.Close()
		}
		shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
			for _, tbl := range level.tables {
				tbl.Close()
			}
			return false
		})
	}
	shard := newShardForLoading(shardInfo, &en.opt)
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&memTables{tables: []*memtable.Table{memtable.NewCFTable(en.numCFs)}}))
	for fid, fm := range shardInfo.files {
		err := en.loadFileFromS3(fid)
		if err != nil {
			return nil, err
		}
		cf := fm.cf
		if cf == -1 {
			filename := sstable.NewFilename(fid, en.opt.Dir)
			file, err := sstable.NewLocalFile(filename, true)
			if err != nil {
				return nil, err
			}
			sl0Tbl, err := sstable.OpenL0Table(file)
			if err != nil {
				return nil, err
			}
			l0Tbls := shard.loadL0Tables()
			l0Tbls.tables = append(l0Tbls.tables, sl0Tbl)
			continue
		}
		level := fm.level
		scf := shard.cfs[cf]
		handler := scf.getLevelHandler(int(level))
		filename := sstable.NewFilename(fid, en.opt.Dir)
		reader, err := newTableFile(filename, en)
		if err != nil {
			return nil, err
		}
		tbl, err := sstable.OpenTable(reader, en.blkCache)
		if err != nil {
			return nil, err
		}
		handler.totalSize += tbl.Size()
		handler.tables = append(handler.tables, tbl)
	}
	l0Tbls := shard.loadL0Tables()
	// Sort the l0 tables by age.
	sort.Slice(l0Tbls.tables, func(i, j int) bool {
		return l0Tbls.tables[i].CommitTS() > l0Tbls.tables[j].CommitTS()
	})
	for cf := 0; cf < len(en.opt.CFs); cf++ {
		scf := shard.cfs[cf]
		for level := 1; level <= len(scf.levels); level++ {
			handler := scf.getLevelHandler(level)
			sortTables(handler.tables)
		}
	}
	en.shardMap.Store(shard.ID, shard)
	log.S().Infof("load shard %d ver %d", shard.ID, shard.Ver)
	return shard, nil
}

func newTableFile(filename string, en *Engine) (sstable.TableFile, error) {
	reader, err := sstable.NewLocalFile(filename, en.blkCache == nil)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// RecoverHandler handles recover a shard's mem-table data from another data source.
type RecoverHandler interface {
	// Recover recovers from the shard's state to the state that is stored in the toState property.
	// So the Engine has a chance to execute pre-split command.
	// If toState is nil, the implementation should recovers to the latest state.
	Recover(en *Engine, shard *Shard, info *ShardMeta) error
}

type localIDAllocator struct {
	latest uint64
}

func (l *localIDAllocator) AllocID(count int) (uint64, error) {
	return atomic.AddUint64(&l.latest, uint64(count)), nil
}

func (en *Engine) Close() error {
	atomic.StoreUint32(&en.closed, 1)
	log.S().Info("closing Engine")
	close(en.flushCh)
	close(en.flushResultCh)
	en.closers.memtable.SignalAndWait()
	if !en.opt.DoNotCompact {
		en.closers.compactors.SignalAndWait()
	}
	en.closers.resourceManager.SignalAndWait()
	if en.opt.S3Options.EndPoint != "" {
		en.closers.s3Client.SignalAndWait()
	}
	return en.dirLock.release()
}

type WriteBatch struct {
	shard         *Shard
	cfConfs       []CFConfig
	entries       [][]*memtable.Entry
	estimatedSize int64
	properties    map[string][]byte
	entryArena    []memtable.Entry
	entryArenaIdx int
}

func (en *Engine) NewWriteBatch(shard *Shard) *WriteBatch {
	return &WriteBatch{
		shard:      shard,
		cfConfs:    en.opt.CFs,
		entries:    make([][]*memtable.Entry, en.numCFs),
		properties: map[string][]byte{},
	}
}

func (wb *WriteBatch) allocEntry(key []byte, val y.ValueStruct) *memtable.Entry {
	if len(wb.entryArena) <= wb.entryArenaIdx {
		wb.entryArena = append(wb.entryArena, memtable.Entry{})
		wb.entryArena = wb.entryArena[:cap(wb.entryArena)]
	}
	e := &wb.entryArena[wb.entryArenaIdx]
	e.Key = key
	e.Value = val
	wb.entryArenaIdx++
	return e
}

func (wb *WriteBatch) Put(cf int, key []byte, val y.ValueStruct) error {
	if wb.cfConfs[cf].Managed {
		if val.Version == 0 {
			return fmt.Errorf("version is zero for managed CF")
		}
	} else {
		if val.Version != 0 {
			return fmt.Errorf("version is not zero for non-managed CF")
		}
	}
	wb.entries[cf] = append(wb.entries[cf], wb.allocEntry(key, val))
	wb.estimatedSize += int64(len(key) + int(val.EncodedSize()) + memtable.EstimateNodeSize)
	return nil
}

func (wb *WriteBatch) Delete(cf byte, key []byte, version uint64) error {
	if wb.cfConfs[cf].Managed {
		if version == 0 {
			return fmt.Errorf("version is zero for managed CF")
		}
	} else {
		if version != 0 {
			return fmt.Errorf("version is not zero for non-managed CF")
		}
	}
	wb.entries[cf] = append(wb.entries[cf], wb.allocEntry(key, y.ValueStruct{Meta: table.BitDelete, Version: version}))
	wb.estimatedSize += int64(len(key) + memtable.EstimateNodeSize)
	return nil
}

func (wb *WriteBatch) SetProperty(key string, val []byte) {
	wb.properties[key] = val
}

func (wb *WriteBatch) EstimatedSize() int64 {
	return wb.estimatedSize
}

func (wb *WriteBatch) NumEntries() int {
	var n int
	for _, entries := range wb.entries {
		n += len(entries)
	}
	return n
}

func (wb *WriteBatch) Reset() {
	for i, entries := range wb.entries {
		wb.entries[i] = entries[:0]
	}
	wb.estimatedSize = 0
	for key := range wb.properties {
		delete(wb.properties, key)
	}
	wb.entryArenaIdx = 0
}

func (wb *WriteBatch) Iterate(cf int, fn func(e *memtable.Entry) (more bool)) {
	for _, e := range wb.entries[cf] {
		if !fn(e) {
			break
		}
	}
}

type SnapAccess struct {
	guard     *epoch.Guard
	shard     *Shard
	cfs       []CFConfig
	hints     []memtable.Hint
	memTables *memTables
	splitting []*memtable.Table
	l0Tables  *l0Tables

	managedReadTS uint64
}

func (s *SnapAccess) Get(cf int, key []byte, version uint64) (*Item, error) {
	if version == 0 {
		version = math.MaxUint64
	}
	vs := s.getValue(cf, key, version)
	if !vs.Valid() {
		return nil, ErrKeyNotFound
	}
	if table.IsDeleted(vs.Meta) {
		return nil, ErrKeyNotFound
	}
	item := new(Item)
	item.key = key
	item.ver = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.val = vs.Value
	return item, nil
}

func (s *SnapAccess) getValue(cf int, key []byte, version uint64) y.ValueStruct {
	keyHash := farm.Fingerprint64(key)
	if s.splitting != nil {
		idx := s.shard.getSplittingIndex(key)
		v := s.splitting[idx].Get(cf, key, version)
		if v.Valid() {
			return v
		}
	}
	for i, memTbl := range s.memTables.tables {
		var v y.ValueStruct
		if i == 0 {
			v = memTbl.GetWithHint(cf, key, version, &s.hints[cf])
		} else {
			v = memTbl.Get(cf, key, version)
		}
		if v.Valid() {
			return v
		}
	}
	for _, tbl := range s.l0Tables.tables {
		v := tbl.Get(cf, key, version, keyHash)
		if v.Valid() {
			return v
		}
	}
	scf := s.shard.cfs[cf]
	for i := 1; i <= len(scf.levels); i++ {
		level := scf.getLevelHandler(i)
		if len(level.tables) == 0 {
			continue
		}
		v := level.get(key, version, keyHash)
		if v.Valid() {
			return v
		}
	}
	return y.ValueStruct{}
}

func (s *SnapAccess) MultiGet(cf int, keys [][]byte, version uint64) ([]*Item, error) {
	if version == 0 {
		version = math.MaxUint64
	}
	items := make([]*Item, len(keys))
	for i, key := range keys {
		item, err := s.Get(cf, key, version)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		items[i] = item
	}
	return items, nil
}

func (s *SnapAccess) Discard() {
	s.guard.Done()
}

func (s *SnapAccess) SetManagedReadTS(ts uint64) {
	s.managedReadTS = ts
}

func (en *Engine) NewSnapAccess(shard *Shard) *SnapAccess {
	guard := en.resourceMgr.Acquire()
	snap := &SnapAccess{
		guard: guard,
		shard: shard,
		cfs:   en.opt.CFs,
		hints: make([]memtable.Hint, len(en.opt.CFs)),
	}
	if shard.isSplitting() {
		snap.splitting = make([]*memtable.Table, len(shard.splittingMemTbls))
		for i := 0; i < len(shard.splittingMemTbls); i++ {
			snap.splitting[i] = shard.loadSplittingMemTable(i)
		}
	}
	snap.memTables = shard.loadMemTables()
	snap.l0Tables = shard.loadL0Tables()
	return snap
}

func (en *Engine) RemoveShard(shardID uint64, removeFile bool) error {
	shardVal, ok := en.shardMap.Load(shardID)
	if !ok {
		return errors.New("shard not found")
	}
	shard := shardVal.(*Shard)
	en.shardMap.Delete(shardID)
	en.removeShardFiles(shard, func(id uint64) bool {
		return removeFile
	})
	return nil
}

func (en *Engine) removeShardFiles(shard *Shard, removeFile func(id uint64) bool) {
	guard := en.resourceMgr.Acquire()
	defer guard.Done()
	guard.Delete([]epoch.Resource{&deletion{res: nil, delete: func() {
		l0s := shard.loadL0Tables()
		for _, l0 := range l0s.tables {
			if removeFile(l0.ID()) {
				if en.s3c != nil {
					en.s3c.SetExpired(l0.ID())
				}
				l0.Delete()
			} else {
				l0.Close()
			}
		}
		shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
			for _, tbl := range level.tables {
				if removeFile(tbl.ID()) {
					if en.s3c != nil {
						en.s3c.SetExpired(tbl.ID())
					}
					tbl.Delete()
				} else {
					tbl.Close()
				}
			}
			return false
		})
	}}})
}

func (en *Engine) GetShard(shardID uint64) *Shard {
	shardVal, ok := en.shardMap.Load(shardID)
	if !ok {
		return nil
	}
	return shardVal.(*Shard)
}

func (en *Engine) GetSplitSuggestion(shardID uint64, splitSize int64) [][]byte {
	shard := en.GetShard(shardID)
	return shard.getSuggestSplitKeys(splitSize)
}

func (en *Engine) Size() int64 {
	var size int64
	var shardCnt int64
	en.shardMap.Range(func(key, value interface{}) bool {
		shard := value.(*Shard)
		size += shard.GetEstimatedSize()
		shardCnt++
		return true
	})
	return size + shardCnt
}

func (en *Engine) NumCFs() int {
	return en.numCFs
}

func (en *Engine) GetOpt() Options {
	return en.opt
}

func (en *Engine) TriggerFlush(shard *Shard, skipCnt int) {
	mems := shard.loadMemTables()
	for i := len(mems.tables) - skipCnt - 1; i > 0; i-- {
		memTbl := mems.tables[i]
		log.S().Infof("%d:%d trigger flush mem table ver:%d", shard.ID, shard.Ver, memTbl.GetVersion())
		en.flushCh <- &flushTask{
			shard: shard,
			tbl:   memTbl,
		}
	}
	if len(mems.tables) == 1 && mems.tables[0].Empty() {
		if !shard.IsInitialFlushed() {
			commitTS := shard.allocCommitTS()
			memTbl := memtable.NewCFTable(en.numCFs)
			memTbl.SetVersion(commitTS)
			en.flushCh <- &flushTask{
				shard: shard,
				tbl:   memTbl,
			}
		}
	}
}
