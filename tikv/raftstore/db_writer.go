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
	"time"

	"github.com/ngaut/unistore/metrics"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rcpb "github.com/pingcap/kvproto/pkg/raft_cmdpb"
)

type engineWriter struct {
	router *router
}

func (writer *engineWriter) Open() {
	// TODO: stub
}

func (writer *engineWriter) Close() {
	// TODO: stub
}

func (writer *engineWriter) NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	return NewCustomWriteBatch(startTS, commitTS, ctx)
}

func (writer *engineWriter) Write(batch mvcc.WriteBatch) error {
	return writer.write(batch, NewCallback())
}

func (writer *engineWriter) write(batch mvcc.WriteBatch, cb *Callback) error {
	cmd := &MsgRaftCmd{
		SendTime: time.Now(),
		Callback: cb,
		Request:  batch.(*customWriteBatch).builder.Build(),
	}
	start := time.Now()
	writer.router.sendRaftCommand(cmd)
	resp := cmd.Callback.Wait()
	waitDoneTime := time.Now()
	metrics.RaftWriterWait.Observe(waitDoneTime.Sub(start).Seconds())
	return writer.checkResponse(resp)
}

func (writer *engineWriter) WritePessimisticLock(batch mvcc.WriteBatch, doneFn func()) error {
	cb := NewCallback()
	cb.respOnProposed = true
	cb.doneFn = doneFn
	return writer.write(batch, cb)
}

type RaftError struct {
	RequestErr *errorpb.Error
}

func (re *RaftError) Error() string {
	return re.RequestErr.String()
}

func (writer *engineWriter) checkResponse(resp *rcpb.RaftCmdResponse) error {
	if resp.GetHeader().GetError() != nil {
		return &RaftError{RequestErr: resp.Header.Error}
	}
	return nil
}

func (writer *engineWriter) DeleteRange(startKey, endKey []byte, latchHandle mvcc.LatchHandle) error {
	return nil // TODO: stub
}

func NewEngineWriter(router *RaftstoreRouter) mvcc.EngineWriter {
	return &engineWriter{
		router: router.router,
	}
}

// TestRaftWriter is used to mock raft write related prewrite and commit operations without
// sending real raft commands
type TestRaftWriter struct {
	engine *Engines
}

func (w *TestRaftWriter) Open() {
}

func (w *TestRaftWriter) Close() {
}

func (w *TestRaftWriter) Write(batch mvcc.WriteBatch) error {
	raftWriteBatch := batch.(*customWriteBatch)
	raftLog := raftWriteBatch.builder.Build()
	applier := new(applier)
	applyCtx := newApplyContext("test", nil, w.engine, nil, NewDefaultConfig())
	applyCtx.execCtx = &applyExecContext{index: RaftInitLogIndex, term: RaftInitLogTerm}
	applier.execWriteCmd(applyCtx, raftLog)
	return nil
}

func (w *TestRaftWriter) WritePessimisticLock(batch mvcc.WriteBatch, doneFn func()) error {
	return w.Write(batch)
}

func (w *TestRaftWriter) DeleteRange(start, end []byte, latchHandle mvcc.LatchHandle) error {
	return nil
}

func (w *TestRaftWriter) NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	return NewCustomWriteBatch(startTS, commitTS, ctx)
}

func NewTestRaftWriter(engine *Engines) mvcc.EngineWriter {
	writer := &TestRaftWriter{
		engine: engine,
	}
	return writer
}

type customWriteBatch struct {
	startTS  uint64
	commitTS uint64
	builder  *raftlog.CustomBuilder
}

func (wb *customWriteBatch) setType(tp raftlog.CustomRaftLogType) {
	oldTp := wb.builder.GetType()
	if oldTp == 0 {
		wb.builder.SetType(tp)
	} else {
		y.Assert(tp == oldTp)
	}
}

func (wb *customWriteBatch) Prewrite(key []byte, lock *mvcc.MvccLock) {
	wb.setType(raftlog.TypePrewrite)
	wb.builder.AppendLock(key, lock.MarshalBinary())
}

func (wb *customWriteBatch) Commit(key []byte, lock *mvcc.MvccLock) {
	wb.setType(raftlog.TypeCommit)
	var val []byte
	if lock != nil {
		val = lock.MarshalBinary()
	}
	wb.builder.AppendCommit(key, val, wb.commitTS)
}

func (wb *customWriteBatch) Rollback(key []byte, deleleLock bool) {
	wb.setType(raftlog.TypeRollback)
	wb.builder.AppendRollback(key, wb.startTS, deleleLock)
}

func (wb *customWriteBatch) PessimisticLock(key []byte, lock *mvcc.MvccLock) {
	wb.setType(raftlog.TypePessimisticLock)
	wb.builder.AppendLock(key, lock.MarshalBinary())
}

func (wb *customWriteBatch) PessimisticRollback(key []byte) {
	wb.setType(raftlog.TypePessimisticRollback)
	wb.builder.AppendKeyOnly(key)
}

func NewCustomWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	header := raftlog.CustomHeader{
		RegionID: ctx.RegionId,
		Epoch:    raftlog.NewEpoch(ctx.RegionEpoch.Version, ctx.RegionEpoch.ConfVer),
		PeerID:   ctx.Peer.Id,
		StoreID:  ctx.Peer.StoreId,
	}
	b := raftlog.NewBuilder(header)
	return &customWriteBatch{
		startTS:  startTS,
		commitTS: commitTS,
		builder:  b,
	}
}
