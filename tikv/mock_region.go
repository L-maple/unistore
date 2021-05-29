package tikv

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ngaut/unistore/tikv/regiontree"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	pdclient "github.com/tikv/pd/client"
	"golang.org/x/net/context"
)

type MockRegionManager struct {
	regionManager

	db         *badger.DB
	regionTree *regiontree.RegionTree
	stores     map[uint64]*metapb.Store
	id         uint64
	clusterID  uint64
	regionSize int64
	closed     uint32
}

func NewMockRegionManager(db *badger.DB, clusterID uint64, opts RegionOptions) *MockRegionManager {
	rm := &MockRegionManager{
		db:         db,
		id:         1,
		clusterID:  clusterID,
		regionSize: opts.RegionSize,
		regionTree: regiontree.NewRegionTree(),
		stores:     make(map[uint64]*metapb.Store),
		regionManager: regionManager{
			regions:   make(map[uint64]*regionCtx),
			storeMeta: new(metapb.Store),
			latches:   newLatches(),
		},
	}
	return rm
}

func (rm *MockRegionManager) Close() error {
	atomic.StoreUint32(&rm.closed, 1)
	return nil
}

func (rm *MockRegionManager) AllocID() uint64 {
	return atomic.AddUint64(&rm.id, 1)
}

func (rm *MockRegionManager) AllocIDs(n int) []uint64 {
	max := atomic.AddUint64(&rm.id, uint64(n))
	ids := make([]uint64, n)
	base := max - uint64(n-1)
	for i := range ids {
		ids[i] = base + uint64(i)
	}
	return ids
}

func (rm *MockRegionManager) GetRegion(id uint64) *metapb.Region {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return proto.Clone(rm.regions[id].meta).(*metapb.Region)
}

func (rm *MockRegionManager) GetRegionByKey(key []byte) (region *metapb.Region, peer *metapb.Peer) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	region = rm.regionTree.GetRegionByKey(key)
	if region == nil || !rm.regionContainsKey(region, key) {
		return nil, nil
	}
	return proto.Clone(region).(*metapb.Region), proto.Clone(region.Peers[0]).(*metapb.Peer)
}

func (rm *MockRegionManager) regionContainsKey(r *metapb.Region, key []byte) bool {
	return bytes.Compare(r.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.GetEndKey()) < 0 || len(r.GetEndKey()) == 0)
}

func (rm *MockRegionManager) regionContainsKeyByEnd(r *metapb.Region, key []byte) bool {
	return bytes.Compare(r.GetStartKey(), key) < 0 &&
		(bytes.Compare(key, r.GetEndKey()) <= 0 || len(r.GetEndKey()) == 0)
}

func (rm *MockRegionManager) Bootstrap(stores []*metapb.Store, region *metapb.Region) error {
	bootstrapped, err := rm.IsBootstrapped()
	if err != nil {
		return err
	}
	if bootstrapped {
		return nil
	}

	regions := make([]*regionCtx, 0, 5)
	rm.mu.Lock()

	// We must in TiDB's tests if we got more than one store.
	// So we use the first one to check requests and put others into stores map.
	rm.storeMeta = stores[0]
	for _, s := range stores {
		rm.stores[s.Id] = s
	}

	region.RegionEpoch.ConfVer = 1
	region.RegionEpoch.Version = 1
	root := newRegionCtx(region, rm.latches, nil)
	rm.regions[region.Id] = root
	rm.regionTree.Put(root.meta)
	regions = append(regions, root)
	rm.mu.Unlock()

	err = rm.saveRegions(regions)
	if err != nil {
		return err
	}

	storeBuf, err := rm.storeMeta.Marshal()
	if err != nil {
		return err
	}
	return rm.db.Update(func(txn *badger.Txn) error {
		return txn.Set(InternalStoreMetaKey, storeBuf)
	})
}

func (rm *MockRegionManager) IsBootstrapped() (bool, error) {
	var item *badger.Item
	err := rm.db.View(func(txn *badger.Txn) error {
		var err2 error
		item, err2 = txn.Get(InternalStoreMetaKey)
		return err2
	})
	if err != nil && err != badger.ErrKeyNotFound {
		return false, err
	}
	return item != nil, nil
}

// Split splits a Region at the key (encoded) and creates new Region.
func (rm *MockRegionManager) Split(regionID, newRegionID uint64, key []byte, peerIDs []uint64, leaderPeerID uint64) {
	_, err := rm.split(regionID, newRegionID, codec.EncodeBytes(nil, key), peerIDs)
	if err != nil {
		panic(err)
	}
}

// SplitRaw splits a Region at the key (not encoded) and creates new Region.
func (rm *MockRegionManager) SplitRaw(regionID, newRegionID uint64, rawKey []byte, peerIDs []uint64, leaderPeerID uint64) *metapb.Region {
	new, err := rm.split(regionID, newRegionID, rawKey, peerIDs)
	if err != nil {
		panic(err)
	}
	return proto.Clone(new).(*metapb.Region)
}

// SplitTable evenly splits the data in table into count regions.
func (rm *MockRegionManager) SplitTable(tableID int64, count int) {
	tableStart := tablecodec.GenTableRecordPrefix(tableID)
	tableEnd := tableStart.PrefixNext()
	keys := rm.calculateSplitKeys(tableStart, tableEnd, count)
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

// SplitIndex evenly splits the data in index into count regions.
func (rm *MockRegionManager) SplitIndex(tableID, indexID int64, count int) {
	indexStart := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	indexEnd := indexStart.PrefixNext()
	keys := rm.calculateSplitKeys(indexStart, indexEnd, count)
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

// SplitKeys evenly splits the start, end key into "count" regions.
func (rm *MockRegionManager) SplitKeys(start, end kv.Key, count int) {
	keys := rm.calculateSplitKeys(start, end, count)
	if _, err := rm.splitKeys(keys); err != nil {
		panic(err)
	}
}

func (rm *MockRegionManager) SplitRegion(req *kvrpcpb.SplitRegionRequest, _ *requestCtx) *kvrpcpb.SplitRegionResponse {
	if _, err := rm.GetRegionFromCtx(req.Context); err != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: err}
	}
	splitKeys := make([][]byte, 0, len(req.SplitKeys))
	for _, rawKey := range req.SplitKeys {
		splitKeys = append(splitKeys, codec.EncodeBytes(nil, rawKey))
	}
	sort.Slice(splitKeys, func(i, j int) bool {
		return bytes.Compare(splitKeys[i], splitKeys[j]) < 0
	})

	newRegions, err := rm.splitKeys(splitKeys)
	if err != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{Message: err.Error()}}
	}

	ret := make([]*metapb.Region, 0, len(newRegions))
	for _, regCtx := range newRegions {
		ret = append(ret, proto.Clone(regCtx.meta).(*metapb.Region))
	}
	return &kvrpcpb.SplitRegionResponse{Regions: ret}
}

func (rm *MockRegionManager) calculateSplitKeys(start, end []byte, count int) [][]byte {
	var keys [][]byte
	err := rm.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		for iter.Seek(start); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.Key()
			if bytes.Compare(key, end) >= 0 {
				break
			}
			keys = append(keys, safeCopy(key))
		}
		return nil
	})
	if err != nil {
		return nil
	}
	splitKeys := make([][]byte, 0, count)
	quotient := len(keys) / count
	remainder := len(keys) % count
	i := 0
	for i < len(keys) {
		regionEntryCount := quotient
		if remainder > 0 {
			remainder--
			regionEntryCount++
		}
		i += regionEntryCount
		if i < len(keys) {
			splitKeys = append(splitKeys, codec.EncodeBytes(nil, keys[i]))
		}
	}
	return splitKeys
}

func (rm *MockRegionManager) splitKeys(keys [][]byte) ([]*regionCtx, error) {
	rm.mu.Lock()
	newRegions := make([]*regionCtx, 0, len(keys))
	rm.regionTree.Iterate(keys[0], nil, func(region *metapb.Region) bool {
		var i int
		for i = 0; i < len(keys); i++ {
			if len(region.EndKey) > 0 && bytes.Compare(keys[i], region.EndKey) >= 0 {
				break
			}
		}
		splits := keys[:i]
		keys = keys[i:]
		if len(splits) == 0 {
			return true
		}

		startKey := region.StartKey
		if bytes.Equal(startKey, splits[0]) {
			splits = splits[1:]
		}
		if len(splits) == 0 {
			return true
		}

		newRegions = append(newRegions, newRegionCtx(&metapb.Region{
			Id:       region.Id,
			StartKey: startKey,
			EndKey:   splits[0],
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: region.RegionEpoch.ConfVer,
				Version: region.RegionEpoch.Version + 1,
			},
			Peers: region.Peers,
		}, rm.latches, nil))

		for i := 0; i < len(splits)-1; i++ {
			newRegions = append(newRegions, newRegionCtx(&metapb.Region{
				Id:          rm.AllocID(),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
				Peers:       []*metapb.Peer{{Id: rm.AllocID(), StoreId: rm.storeMeta.Id}},
				StartKey:    splits[i],
				EndKey:      splits[i+1],
			}, rm.latches, nil))
		}

		if !bytes.Equal(splits[len(splits)-1], region.EndKey) {
			newRegions = append(newRegions, newRegionCtx(&metapb.Region{
				Id:          rm.AllocID(),
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
				Peers:       []*metapb.Peer{{Id: rm.AllocID(), StoreId: rm.storeMeta.Id}},
				StartKey:    splits[len(splits)-1],
				EndKey:      region.EndKey,
			}, rm.latches, nil))
		}
		return true
	})
	for _, region := range newRegions {
		rm.regions[region.meta.Id] = region
		rm.regionTree.Put(region.meta)
	}
	rm.mu.Unlock()
	return newRegions, rm.saveRegions(newRegions)
}

func (rm *MockRegionManager) split(regionID, newRegionID uint64, key []byte, peerIDs []uint64) (*metapb.Region, error) {
	rm.mu.RLock()
	old := rm.regions[regionID]
	rm.mu.RUnlock()
	oldRegion := old.meta
	leftMeta := &metapb.Region{
		Id:       oldRegion.Id,
		StartKey: oldRegion.StartKey,
		EndKey:   key,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: oldRegion.RegionEpoch.ConfVer,
			Version: oldRegion.RegionEpoch.Version + 1,
		},
		Peers: oldRegion.Peers,
	}
	left := newRegionCtx(leftMeta, rm.latches, nil)

	peers := make([]*metapb.Peer, 0, len(leftMeta.Peers))
	for i, p := range leftMeta.Peers {
		peers = append(peers, &metapb.Peer{
			StoreId: p.StoreId,
			Id:      peerIDs[i],
		})
	}
	rightMeta := &metapb.Region{
		Id:       newRegionID,
		StartKey: key,
		EndKey:   oldRegion.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
	right := newRegionCtx(rightMeta, rm.latches, nil)

	if err1 := rm.saveRegions([]*regionCtx{left, right}); err1 != nil {
		return nil, err1
	}

	rm.mu.Lock()
	rm.regions[left.meta.Id] = left
	rm.regionTree.Put(left.meta)
	rm.regions[right.meta.Id] = right
	rm.regionTree.Put(right.meta)
	rm.mu.Unlock()

	return right.meta, nil
}

func (rm *MockRegionManager) saveRegions(regions []*regionCtx) error {
	if atomic.LoadUint32(&rm.closed) == 1 {
		return nil
	}
	return rm.db.Update(func(txn *badger.Txn) error {
		for _, r := range regions {
			y.Assert(txn.Set(InternalRegionMetaKey(r.meta.Id), r.marshal()) == nil)
		}
		return nil
	})
}

func (rm *MockRegionManager) ScanRegions(startKey, endKey []byte, limit int) []*pdclient.Region {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	regions := make([]*pdclient.Region, 0, len(rm.regions))
	rm.regionTree.Iterate(startKey, endKey, func(region *metapb.Region) bool {
		if len(regions) == 0 && bytes.Equal(region.EndKey, startKey) {
			return true
		}

		regions = append(regions, &pdclient.Region{
			Meta:   proto.Clone(region).(*metapb.Region),
			Leader: proto.Clone(region.Peers[0]).(*metapb.Peer),
		})

		return !(limit > 0 && len(regions) >= limit)
	})
	return regions
}

func (rm *MockRegionManager) GetAllStores() []*metapb.Store {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	stores := make([]*metapb.Store, 0, len(rm.stores))
	for _, store := range rm.stores {
		stores = append(stores, proto.Clone(store).(*metapb.Store))
	}
	return stores
}

// AddStore adds a new Store to the cluster.
func (rm *MockRegionManager) AddStore(storeID uint64, addr string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.stores[storeID] = &metapb.Store{
		Id:      storeID,
		Address: addr,
	}
}

// RemoveStore removes a Store from the cluster.
func (rm *MockRegionManager) RemoveStore(storeID uint64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	delete(rm.stores, storeID)
}

type MockPD struct {
	rm          *MockRegionManager
	gcSafePoint uint64
}

func NewMockPD(rm *MockRegionManager) *MockPD {
	return &MockPD{
		rm: rm,
	}
}

func (pd *MockPD) GetClusterID(ctx context.Context) uint64 {
	return pd.rm.clusterID
}

func (pd *MockPD) AllocID(ctx context.Context) (uint64, error) {
	return pd.rm.AllocID(), nil
}

func (pd *MockPD) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error) {
	if err := pd.rm.Bootstrap([]*metapb.Store{store}, region); err != nil {
		return nil, err
	}
	return &pdpb.BootstrapResponse{
		Header: &pdpb.ResponseHeader{ClusterId: pd.rm.clusterID},
	}, nil
}

func (pd *MockPD) IsBootstrapped(ctx context.Context) (bool, error) {
	return pd.rm.IsBootstrapped()
}

func (pd *MockPD) PutStore(ctx context.Context, store *metapb.Store) error {
	pd.rm.mu.Lock()
	defer pd.rm.mu.Unlock()
	pd.rm.stores[store.Id] = store
	return nil
}

func (pd *MockPD) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	pd.rm.mu.RLock()
	defer pd.rm.mu.RUnlock()
	return proto.Clone(pd.rm.stores[storeID]).(*metapb.Store), nil
}

func (pd *MockPD) GetRegion(ctx context.Context, key []byte) (*pdclient.Region, error) {
	r, p := pd.rm.GetRegionByKey(key)
	return &pdclient.Region{Meta: r, Leader: p}, nil
}

func (pd *MockPD) GetRegionByID(ctx context.Context, regionID uint64) (*pdclient.Region, error) {
	pd.rm.mu.RLock()
	defer pd.rm.mu.RUnlock()

	r := pd.rm.regions[regionID]
	if r == nil {
		return nil, nil
	}
	return &pdclient.Region{Meta: proto.Clone(r.meta).(*metapb.Region), Leader: proto.Clone(r.meta.Peers[0]).(*metapb.Peer)}, nil
}

func (pd *MockPD) ReportRegion(*pdpb.RegionHeartbeatRequest) {}

func (pd *MockPD) AskSplit(ctx context.Context, region *metapb.Region) (*pdpb.AskSplitResponse, error) {
	panic("unimplemented")
}

func (pd *MockPD) AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (*pdpb.AskBatchSplitResponse, error) {
	panic("unimplemented")
}

func (pd *MockPD) ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error {
	panic("unimplemented")
}

func (pd *MockPD) SetRegionHeartbeatResponseHandler(h func(*pdpb.RegionHeartbeatResponse)) {
	panic("unimplemented")
}

func (pd *MockPD) GetGCSafePoint(ctx context.Context) (uint64, error) {
	return atomic.LoadUint64(&pd.gcSafePoint), nil
}

func (pd *MockPD) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	for {
		old := atomic.LoadUint64(&pd.gcSafePoint)
		if safePoint <= old {
			return old, nil
		}
		if atomic.CompareAndSwapUint64(&pd.gcSafePoint, old, safePoint) {
			return safePoint, nil
		}
	}
}

func (pd *MockPD) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error { return nil }

// Use global variables to prevent pdClients from creating duplicate timestamps.
var tsMu = struct {
	sync.Mutex
	physicalTS int64
	logicalTS  int64
}{}

func (pd *MockPD) GetTS(ctx context.Context) (int64, int64, error) {
	p, l := GetTS()
	return p, l, nil
}

func GetTS() (int64, int64) {
	tsMu.Lock()
	defer tsMu.Unlock()

	ts := time.Now().UnixNano() / int64(time.Millisecond)
	if tsMu.physicalTS >= ts {
		tsMu.logicalTS++
	} else {
		tsMu.physicalTS = ts
		tsMu.logicalTS = 0
	}
	return tsMu.physicalTS, tsMu.logicalTS
}

func (pd *MockPD) GetAllStores(ctx context.Context, opts ...pdclient.GetStoreOption) ([]*metapb.Store, error) {
	return pd.rm.GetAllStores(), nil
}

func (pd *MockPD) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*pdclient.Region, error) {
	regions := pd.rm.ScanRegions(startKey, endKey, limit)
	return regions, nil
}

func (pd *MockPD) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

func (pd *MockPD) Close() {}
