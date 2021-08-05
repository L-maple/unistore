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

package tikv

import (
  "context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

var (
  ErrNotImplemented = errors.New("Not Implemented")
)

func (svr *Server) ClearContext(ctx context.Context, req *kvrpcpb.ClearContextRequest) (*kvrpcpb.ClearContextResponse, error) {
  return &kvrpcpb.ClearContextResponse{}, ErrNotImplemented 
}

func (svr *Server) Compaction(ctx context.Context, req *kvrpcpb.CompactionRequest) (*kvrpcpb.CompactionResponse, error) {
  return &kvrpcpb.CompactionResponse{}, ErrNotImplemented
}

func (svr *Server) CreateRegion(ctx context.Context, req *kvrpcpb.CreateRegionRequest) (*kvrpcpb.CreateRegionResponse, error) {
  return &kvrpcpb.CreateRegionResponse{}, ErrNotImplemented
}

func (svr *Server) CreateTable(ctx context.Context, req *kvrpcpb.CreateTableRequest) (*kvrpcpb.CreateTableResponse, error) {
  return &kvrpcpb.CreateTableResponse{}, ErrNotImplemented
}

func (svr *Server) DeleteRegion(ctx context.Context, req *kvrpcpb.DeleteTableRequest) (*kvrpcpb.DeleteTableResponse, error) {
  return &kvrpcpb.DeleteTableResponse{}, ErrNotImplemented
}

func (svr *Server) DestroyRegion(ctx context.Context, req *kvrpcpb.DestroyRegionRequest) (*kvrpcpb.DestroyRegionResponse, error) {
  return &kvrpcpb.DestroyRegionResponse{}, ErrNotImplemented
}

func (svr *Server) ModifyLogModule(ctx context.Context, req *kvrpcpb.ModifyLogModuleRequest) (*kvrpcpb.ModifyLogModuleResponse, error) {
  return &kvrpcpb.ModifyLogModuleResponse{}, ErrNotImplemented
}

func (svr *Server) Ping(ctx context.Context, req *kvrpcpb.PingRequest) (*kvrpcpb.PingResponse, error) {
  return &kvrpcpb.PingResponse{}, ErrNotImplemented
}

func (svr *Server) StmtRollback(ctx context.Context, req *kvrpcpb.StmtRollbackRequest) (*kvrpcpb.StmtRollbackResponse, error) {
  return &kvrpcpb.StmtRollbackResponse{}, ErrNotImplemented
}

func (svr *Server) TxnPrepare(ctx context.Context, req *kvrpcpb.TxnPrepareRequest) (*kvrpcpb.TxnPrepareResponse, error) {
  return &kvrpcpb.TxnPrepareResponse{}, ErrNotImplemented
}

func (svr *Server) TxnCommit(ctx context.Context, req *kvrpcpb.TxnCommitRequest) (*kvrpcpb.TxnCommitResponse, error) {
  return &kvrpcpb.TxnCommitResponse{}, ErrNotImplemented
}

func (svr *Server) TxnClear(ctx context.Context, req *kvrpcpb.TxnClearRequest) (*kvrpcpb.TxnClearResponse, error) {
  return &kvrpcpb.TxnClearResponse{}, ErrNotImplemented
}

func (svr *Server) TxnRollback(ctx context.Context, req *kvrpcpb.TxnRollbackRequest) (*kvrpcpb.TxnRollbackResponse, error) {
  return &kvrpcpb.TxnRollbackResponse{}, ErrNotImplemented
}
