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

package config

import (
	"time"

	"github.com/pingcap/log"
)

type Config struct {
	Server         Server         `toml:"server"` // Unistore server options
	Engine         Engine         `toml:"engine"` // Engine options.
	RaftEngine     Engine         `toml:"raft-engine"`
	RaftStore      RaftStore      `toml:"raftstore"`       // RaftStore configs
	PessimisticTxn PessimisticTxn `toml:"pessimistic-txn"` // Pessimistic txn related
}

type Server struct {
	PDAddr      string `toml:"pd-addr"`
	StoreAddr   string `toml:"store-addr"`
	StatusAddr  string `toml:"status-addr"`
	LogLevel    string `toml:"log-level"`
	RegionSize  int64  `toml:"region-size"` // Average region size.
	MaxProcs    int    `toml:"max-procs"`   // Max CPU cores to use, set 0 to use all CPU cores in the machine.
	LogfilePath string `toml:"log-file"`    // Log file path for unistore server
}

type RaftStore struct {
	PdHeartbeatTickInterval  string `toml:"pd-heartbeat-tick-interval"`  // pd-heartbeat-tick-interval in seconds
	RaftStoreMaxLeaderLease  string `toml:"raft-store-max-leader-lease"` // raft-store-max-leader-lease in milliseconds
	RaftBaseTickInterval     string `toml:"raft-base-tick-interval"`     // raft-base-tick-interval in milliseconds
	RaftHeartbeatTicks       int    `toml:"raft-heartbeat-ticks"`        // raft-heartbeat-ticks times
	RaftElectionTimeoutTicks int    `toml:"raft-election-timeout-ticks"` // raft-election-timeout-ticks times
}

type Engine struct {
	DBPath           string `toml:"db-path"`            // Directory to store the data in. Should exist and be writable.
	ValueThreshold   int    `toml:"value-threshold"`    // If value size >= this threshold, only store value offsets in tree.
	MaxMemTableSize  int64  `toml:"max-mem-table-size"` // Each mem table is at most this size.
	MaxTableSize     int64  `toml:"max-table-size"`     // Each table file is at most this size.
	L1Size           int64  `toml:"l1-size"`
	NumMemTables     int    `toml:"num-mem-tables"`      // Maximum number of tables to keep in memory, before stalling.
	NumL0Tables      int    `toml:"num-L0-tables"`       // Maximum number of Level 0 tables before we start compacting.
	NumL0TablesStall int    `toml:"num-L0-tables-stall"` // Maximum number of Level 0 tables before stalling.
	VlogFileSize     int64  `toml:"vlog-file-size"`      // Value log file size.

	// 	Sync all writes to disk. Setting this to true would slow down data loading significantly.")
	SyncWrite      bool  `toml:"sync-write"`
	NumCompactors  int   `toml:"num-compactors"`
	SurfStartLevel int   `toml:"surf-start-level"`
	BlockCacheSize int64 `toml:"block-cache-size"`

	// Only used in tests.
	VolatileMode bool

	CompactL0WhenClose bool      `toml:"compact-l0-when-close"`
	S3                 S3Options `toml:"s3"`
}

type S3Options struct {
	Endpoint   string `toml:"endpoint"`
	KeyID      string `toml:"key-id"`
	SecretKey  string `toml:"secret-key"`
	Bucket     string `toml:"bucket"`
	InstanceID uint32 `toml:"instance-id"`
	Region     string `toml:"region"`
}

type PessimisticTxn struct {
	// The default and maximum delay in milliseconds before responding to TiDB when pessimistic
	// transactions encounter locks
	WaitForLockTimeout int64 `toml:"wait-for-lock-timeout"`

	// The duration between waking up lock waiter, in milliseconds
	WakeUpDelayDuration int64 `toml:"wake-up-delay-duration"`
}

const MB = 1024 * 1024

var DefaultConf = Config{
	Server: Server{
		PDAddr:      "127.0.0.1:2379",
		StoreAddr:   "127.0.0.1:9191",
		StatusAddr:  "127.0.0.1:9291",
		RegionSize:  64 * MB,
		LogLevel:    "info",
		MaxProcs:    0,
		LogfilePath: "",
	},
	RaftStore: RaftStore{
		PdHeartbeatTickInterval:  "20s",
		RaftStoreMaxLeaderLease:  "9s",
		RaftBaseTickInterval:     "1s",
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
	},
	Engine: Engine{
		DBPath:             "/tmp/badger",
		ValueThreshold:     256,
		MaxMemTableSize:    64 * MB,
		MaxTableSize:       8 * MB,
		NumMemTables:       3,
		NumL0Tables:        4,
		NumL0TablesStall:   8,
		VlogFileSize:       256 * MB,
		NumCompactors:      3,
		SurfStartLevel:     8,
		L1Size:             512 * MB,
		BlockCacheSize:     0, // 0 means disable block cache, use mmap to access sst.
		CompactL0WhenClose: true,
	},
	RaftEngine: Engine{
		DBPath:             "/tmp/badger",
		ValueThreshold:     256,
		MaxMemTableSize:    128 * MB,
		MaxTableSize:       16 * MB,
		NumMemTables:       3,
		NumL0Tables:        4,
		NumL0TablesStall:   10,
		VlogFileSize:       256 * MB,
		NumCompactors:      3,
		SurfStartLevel:     8,
		L1Size:             512 * MB,
		BlockCacheSize:     0, // 0 means disable block cache, use mmap to access sst.
		CompactL0WhenClose: true,
	},
	PessimisticTxn: PessimisticTxn{
		WaitForLockTimeout:  1000, // 1000ms same with tikv default value
		WakeUpDelayDuration: 100,  // 100ms same with tikv default value
	},
}

// parseDuration parses duration argument string.
func ParseDuration(durationStr string) time.Duration {
	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		dur, err = time.ParseDuration(durationStr + "s")
	}
	if err != nil || dur < 0 {
		log.S().Fatalf("invalid duration=%v", durationStr)
	}
	return dur
}
