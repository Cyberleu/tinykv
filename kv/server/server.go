package server

import (
	"bytes"
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	var res *kvrpcpb.GetResponse = new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts < req.Version {
		res.Error = &kvrpcpb.KeyError{Locked: lock.Info(req.Key)}
		return res, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		res.NotFound = true
	}
	res.Value = value
	return res, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var res *kvrpcpb.PrewriteResponse = new(kvrpcpb.PrewriteResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mut := range req.Mutations {
		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != txn.StartTS {
			res.Errors = append(res.Errors, &kvrpcpb.KeyError{Locked: lock.Info(mut.Key)})
			return res, nil
		}
		// 锁检测
		lock = &mvcc.Lock{Primary: req.PrimaryLock, Ts: req.StartVersion, Ttl: req.LockTtl, Kind: mvcc.WriteKindPut}
		pr_lock := &kvrpcpb.RawPutRequest{Key: mut.Key, Value: lock.ToBytes(), Cf: engine_util.CfLock}
		res_lock, err := server.RawPut(nil, pr_lock)
		if err != nil {
			return nil, err
		}
		if res_lock.RegionError != nil {
			res.RegionError = res_lock.RegionError
			return res, nil
		}
		_, ts, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			return nil, err
		}
		// 检测写冲突
		if ts > req.StartVersion {
			res.Errors = append(res.Errors, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{StartTs: req.StartVersion, ConflictTs: ts, Key: mut.Key}})
			// 若存在写冲突，则释放锁
			dr_lock := &kvrpcpb.RawDeleteRequest{Key: mut.Key, Cf: engine_util.CfLock}
			res_lock, err := server.RawDelete(nil, dr_lock)
			if err != nil {
				return nil, err
			}
			if res_lock.RegionError != nil {
				res.RegionError = res_lock.RegionError
				return res, nil
			}
			return res, nil
		}
		switch mut.Op {
		case kvrpcpb.Op_Put:
			pr_default := &kvrpcpb.RawPutRequest{Key: mvcc.EncodeKey(mut.Key, txn.StartTS), Value: mut.Value, Cf: engine_util.CfDefault}
			res_default, err := server.RawPut(nil, pr_default)
			if err != nil {
				return nil, err
			}
			if res_default.RegionError != nil {
				res.RegionError = res_default.RegionError
				return res, nil
			}
		case kvrpcpb.Op_Del:
			pr_default := &kvrpcpb.RawDeleteRequest{Key: mvcc.EncodeKey(mut.Key, txn.StartTS), Cf: engine_util.CfDefault}
			res_default, err := server.RawDelete(nil, pr_default)
			if err != nil {
				return nil, err
			}
			if res_default.RegionError != nil {
				res.RegionError = res_default.RegionError
				return res, nil
			}
		}
	}
	return res, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	var res *kvrpcpb.CommitResponse = new(kvrpcpb.CommitResponse)
	reader, err := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if err != nil {
		return nil, err
	}
	iter1 := reader.IterCF(engine_util.CfDefault)
	iter2 := reader.IterCF(engine_util.CfLock)
	iter3 := reader.IterCF(engine_util.CfWrite)
	defer iter1.Close()
	defer iter2.Close()
	defer iter3.Close()
	for _, key := range req.Keys {
		iter2.Seek(key)
		write, ts, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil && write.StartTS == req.StartVersion {
			iter1.Seek(mvcc.EncodeKey(key, req.CommitVersion))
			if write.Kind == mvcc.WriteKindRollback {
				// 提交roll back的事务
				res.Error = &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{StartTs: req.StartVersion, ConflictTs: ts, Key: key}, Retryable: "commit roll back conflicts"}
			}
			// 重复commit
			break
		}
		// 与其他事务写冲突
		if ts > req.StartVersion {
			res.Error = &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{StartTs: req.StartVersion, ConflictTs: ts, Key: key}, Retryable: "other transaction write conflicts"}
			return res, nil
		}
		if iter2.Valid() {
			value2, err := iter2.Item().Value()
			if err != nil {
				return nil, err
			}
			lock, err := mvcc.ParseLock(value2)
			if err != nil {
				return nil, err
			}
			if bytes.Compare(iter2.Item().Key(), key) == 0 && lock.Ts == req.StartVersion && ts < req.StartVersion {
				value1, err := reader.GetCF(engine_util.CfDefault, mvcc.EncodeKey(key, req.StartVersion))
				if err != nil {
					return nil, err
				}
				write := &mvcc.Write{StartTS: req.StartVersion}
				if value1 != nil {
					write.Kind = mvcc.WriteKindPut
				} else {
					write.Kind = mvcc.WriteKindDelete
				}
				pr_write := &kvrpcpb.RawPutRequest{Key: mvcc.EncodeKey(key, req.CommitVersion), Value: write.ToBytes(), Cf: engine_util.CfWrite}
				res_default, err := server.RawPut(nil, pr_write)
				if err != nil {
					return nil, err
				}
				if res_default.RegionError != nil {
					res.RegionError = res_default.RegionError
					return res, nil
				}
			} else {
				// 与其他事务的pre-writen提交冲突
				res.Error = &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{StartTs: req.StartVersion, ConflictTs: ts, Key: key}, Retryable: "other pre-writen transaction conflicts"}
				return res, nil
			}
		}
	}
	// commit成功， 释放所有锁
	for _, key := range req.Keys {
		dr_lock := &kvrpcpb.RawDeleteRequest{Key: key, Cf: engine_util.CfLock}
		res_lock, err := server.RawDelete(nil, dr_lock)
		if err != nil {
			return nil, err
		}
		if res_lock.RegionError != nil {
			res.RegionError = res_lock.RegionError
		}
	}
	return res, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	var res *kvrpcpb.ScanResponse = new(kvrpcpb.ScanResponse)
	count := 0
	reader, err := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	if err != nil {
		return nil, err
	}
	scanner := mvcc.NewScanner(req.StartKey, txn)
	for {
		if count == int(req.Limit) {
			break
		}
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			scanner.Close()
			break
		}
		res.Pairs = append(res.Pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		count++
	}
	return res, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	var res *kvrpcpb.CheckTxnStatusResponse = new(kvrpcpb.CheckTxnStatusResponse)
	reader, err := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	if err != nil {
		return nil, err
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if write != nil && (write.Kind == mvcc.WriteKindPut || write.Kind == mvcc.WriteKindDelete) {
		// 事务为已提交状态
		res.Action = kvrpcpb.Action_NoAction
		res.CommitVersion = ts
	} else if write != nil && write.Kind == mvcc.WriteKindRollback {
		// 事务为已回滚状态
		res.Action = kvrpcpb.Action_NoAction
		res.CommitVersion = uint64(0)
	} else {
		if lock == nil {
			// pre-write丢失，进行回滚
			res.Action = kvrpcpb.Action_LockNotExistRollback
			rollback_res, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{StartVersion: req.LockTs, Keys: [][]byte{req.PrimaryKey}})
			if err != nil {
				return nil, err
			}
			res.RegionError = rollback_res.RegionError
		} else if lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			res.Action = kvrpcpb.Action_TTLExpireRollback
			rollback_res, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{StartVersion: req.LockTs, Keys: [][]byte{req.PrimaryKey}})
			if err != nil {
				return nil, err
			}
			res.RegionError = rollback_res.RegionError
		} else {
			res.Action = kvrpcpb.Action_NoAction
		}
	}
	return res, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	var res *kvrpcpb.BatchRollbackResponse = new(kvrpcpb.BatchRollbackResponse)
	reader, err := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if err != nil {
		return nil, err
	}
	for _, key := range req.Keys {
		// v2, err := reader.GetCF(engine_util.CfDefault, mvcc.EncodeKey(key, req.StartVersion))
		// if err != nil {
		// 	return nil, err
		// }
		// v2, err := reader.GetCF(engine_util.CfLock, key)
		// if err != nil {
		// 	return nil, err
		// }
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		// 回滚已提交的事务报错
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			res.Error = &kvrpcpb.KeyError{Abort: "roll back commited transaxtion"}
			return res, nil
		}
		// 回滚已回滚的事务
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}
		// 回滚default
		dr_default := &kvrpcpb.RawDeleteRequest{Key: mvcc.EncodeKey(key, req.StartVersion), Cf: engine_util.CfDefault}
		res_default, err := server.RawDelete(nil, dr_default)
		if err != nil {
			return nil, err
		}
		if res_default.RegionError != nil {
			res.RegionError = res_default.RegionError
		}
		// 进行write操作
		write = &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback}
		pr_write := &kvrpcpb.RawPutRequest{Key: mvcc.EncodeKey(key, req.StartVersion), Value: write.ToBytes(), Cf: engine_util.CfWrite}
		res_write, err := server.RawPut(nil, pr_write)
		if err != nil {
			return res, nil
		}
		if res_write.RegionError != nil {
			res.RegionError = res_write.RegionError
		}
	}
	// 回滚lock
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			continue
		}
		dr_lock := &kvrpcpb.RawDeleteRequest{Key: key, Cf: engine_util.CfLock}
		res_lock, err := server.RawDelete(nil, dr_lock)
		if err != nil {
			return nil, err
		}
		if res_lock.RegionError != nil {
			res.RegionError = res_lock.RegionError
		}
	}
	return res, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	var res *kvrpcpb.ResolveLockResponse = new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(req.Context)
	var keys [][]byte
	// txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if err != nil {
		return nil, err
	}
	iter2 := reader.IterCF(engine_util.CfLock)
	iter2.Close()
	// 获得该事务下的所有key
	for ; iter2.Valid(); iter2.Next() {
		value2, err := iter2.Item().Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(value2)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == req.StartVersion {
			keys = append(keys, iter2.Item().Key())
		}
	}
	// 执行批量commit
	if req.CommitVersion > req.StartVersion {
		commit_res, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{StartVersion: req.StartVersion, Keys: keys, CommitVersion: req.CommitVersion})
		if err != nil {
			return nil, err
		}
		res.Error = commit_res.Error
		res.RegionError = commit_res.RegionError
	} else {
		// 执行批量roll back
		rollback_res, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{StartVersion: req.StartVersion, Keys: keys})
		if err != nil {
			return nil, err
		}
		res.Error = rollback_res.Error
		res.RegionError = rollback_res.RegionError
	}
	return res, nil
}

// func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
// 	// Your Code Here (4C).
// 	var res *kvrpcpb.ResolveLockResponse = new(kvrpcpb.ResolveLockResponse)
// 	reader, err := server.storage.Reader(req.Context)
// 	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
// 	if err != nil {
// 		return nil, err
// 	}
// 	iter1 := reader.IterCF(engine_util.CfDefault)
// 	defer iter1.Close()
// 	var keys [][]byte
// 	for ; iter1.Valid(); iter1.Next() {
// 		ts := mvcc.DecodeTimestamp(iter1.Item().Key())
// 		if ts == req.StartVersion {
// 			lock, err := txn.GetLock(mvcc.DecodeUserKey(iter1.Item().Key()))
// 			if err != nil {
// 				return nil, err
// 			}
// 			if lock == nil {
// 				continue
// 			}
// 			write, _, err := txn.CurrentWrite(iter1.Item().Key())
// 			// 全部提交
// 			if write == nil {
// 				keys = append(keys, iter1.Item().Key())
// 				if req.CommitVersion > req.StartVersion {
// 					write = &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindPut}
// 				} else {
// 					write = &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback}

// 				}
// 				pr_write := &kvrpcpb.RawPutRequest{Key: mvcc.EncodeKey(iter1.Item().Key(), req.StartVersion), Value: write.ToBytes(), Cf: engine_util.CfWrite}
// 				res_write, err := server.RawPut(nil, pr_write)
// 				if err != nil {
// 					return res, nil
// 				}
// 				if res_write.RegionError != nil {
// 					res.RegionError = res_write.RegionError
// 				}
// 			}

// 		}
// 	}

// 	for _, key := range keys {
// 		dr_lock := &kvrpcpb.RawDeleteRequest{Key: key, Cf: engine_util.CfLock}
// 		res_lock, err := server.RawDelete(nil, dr_lock)
// 		if err != nil {
// 			return nil, err
// 		}
// 		if res_lock.RegionError != nil {
// 			res.RegionError = res_lock.RegionError
// 		}
// 	}
// 	return res, nil
// }

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

// func (server *Server) ReleaseAllLocks(keys [][]byte) error {
// 	var err error
// 	for _, key := range keys {
// 		dr_lock := &kvrpcpb.RawDeleteRequest{Key: key, Cf: engine_util.CfLock}
// 		res_lock, err := server.RawDelete(nil, dr_lock)
// 		if err != nil {
// 			continur
// 		}
// 		if res_lock.RegionError != nil {
// 			res.RegionError = res_lock.RegionError
// 			return res, nil
// 		}
// 	}
// 	return nil
// }
