// Copyright 2019-present PingCAP, Inc.
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

package raftstore

import (
	"github.com/ngaut/unistore/enginepb"
	"github.com/pingcap/log"
	"time"

	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
)

type MsgType int64

const (
	MsgTypeNull            MsgType = 0
	MsgTypeRaftMessage     MsgType = 1
	MsgTypeRaftCmd         MsgType = 2
	MsgTypeSplitRegion     MsgType = 3
	MsgTypeComputeResult   MsgType = 4
	MsgTypeHalfSplitRegion MsgType = 8
	MsgTypeMergeResult     MsgType = 9
	MsgTypeTick            MsgType = 12
	MsgTypeStart           MsgType = 14
	MsgTypeApplyRes        MsgType = 15
	MsgTypeNoop            MsgType = 16

	MsgTypeStoreRaftMessage MsgType = 101

	MsgTypeStoreTick  MsgType = 106
	MsgTypeStoreStart MsgType = 107

	MsgTypeApply             MsgType = 301
	MsgTypeApplyRegistration MsgType = 302
	MsgTypeApplyProposal     MsgType = 303
	MsgTypeApplyCatchUpLogs  MsgType = 304
	MsgTypeApplyLogsUpToDate MsgType = 305
	MsgTypeApplyDestroy      MsgType = 306
	MsgTypeApplyResume       MsgType = 307

	// MsgTypeGenerateEngineChangeSet is generated by local engine, it generates a meta change raft log.
	MsgTypeGenerateEngineChangeSet MsgType = 401
	MsgTypeApplyChangeSetResult    MsgType = 402
	MsgTypeWaitFollowerSplitFiles  MsgType = 403

	msgDefaultChanSize = 1024
)

type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}

func NewPeerMsg(tp MsgType, regionID uint64, data interface{}) Msg {
	return Msg{Type: tp, RegionID: regionID, Data: data}
}

func NewMsg(tp MsgType, data interface{}) Msg {
	return Msg{Type: tp, Data: data}
}

type Callback struct {
	respCh chan *raft_cmdpb.RaftCmdResponse
	doneFn func()

	// If respOnProposed is true, we response early after propose instead of after apply.
	respOnProposed bool
}

func (cb *Callback) MaybeResponseOnProposed(resp *raft_cmdpb.RaftCmdResponse) {
	if cb != nil && cb.respOnProposed {
		cb.respCh <- resp
		log.S().Info("response on proposed")
	}
}

func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb != nil {
		cb.respCh <- resp
		if cb.doneFn != nil {
			cb.doneFn()
		}
	}
}

func (cb *Callback) Wait() *raft_cmdpb.RaftCmdResponse {
	return <-cb.respCh
}

func NewCallback() *Callback {
	cb := &Callback{}
	cb.respCh = make(chan *raft_cmdpb.RaftCmdResponse, 2)
	return cb
}

type PeerTick int

const (
	PeerTickRaft             PeerTick = 0
	PeerTickRaftLogGC        PeerTick = 1
	PeerTickSplitRegionCheck PeerTick = 2
	PeerTickPdHeartbeat      PeerTick = 3
	PeerTickCheckMerge       PeerTick = 4
	PeerTickPeerStaleState   PeerTick = 5
)

type StoreTick int

const (
	StoreTickPdStoreHeartbeat StoreTick = 0
	StoreTickConsistencyCheck StoreTick = 1
)

type MsgRaftCmd struct {
	SendTime time.Time
	Request  raftlog.RaftLog
	Callback *Callback
}

type MsgSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
	// It's an encoded key.
	// TODO: support meta key.
	SplitKeys [][]byte
	Callback  *Callback
}

type MsgComputeHashResult struct {
	Index uint64
	Hash  []byte
}

type MsgHalfSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
}

type MsgMergeResult struct {
	TargetPeer *metapb.Peer
	Stale      bool
}

type MsgWaitFollowerSplitFiles struct {
	SplitKeys [][]byte
	Callback  *Callback
}

type MsgApplyChangeSetResult struct {
	change *enginepb.ChangeSet
	err    error
}

func newApplyMsg(apply *apply) Msg {
	return Msg{Type: MsgTypeApply, Data: apply}
}
