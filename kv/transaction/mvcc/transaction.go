package mvcc

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{Key: EncodeKey(key, ts), Value: write.ToBytes(), Cf: engine_util.CfWrite}})
	// Your Code Here (4A).
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if value != nil {
		return ParseLock(value)
	}
	// for i := len(txn.writes) - 1; i >= 0; i-- {
	// 	write := txn.writes[i]
	// 	var lock *Lock
	// 	if write.Cf() == engine_util.CfLock{
	// 		lock , err = ParseLock(write.Value())
	// 		if err != nil{
	// 			return nil, err
	// 		}

	// 		if write.Type() == storage.PUT{

	// 		} else {

	// 		}
	// 	}
	// }
	// Your Code Here (4A).
	return nil, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{Key: key, Value: lock.ToBytes(), Cf: engine_util.CfLock}})
	// Your Code Here (4A).
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{Key: key, Cf: engine_util.CfLock}})
	// Your Code Here (4A).
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	iter1 := txn.Reader.IterCF(engine_util.CfWrite)
	iter2 := txn.Reader.IterCF(engine_util.CfDefault)
	defer iter1.Close()
	defer iter2.Close()
	for iter1.Seek(EncodeKey(key, txn.StartTS)); iter1.Valid(); iter1.Next() {
		if bytes.Compare(DecodeUserKey(iter1.Item().Key()), key) != 0 {
			break
		}
		value, err := iter1.Item().Value()
		if err != nil {
			return nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, err
		}
		if write.Kind == WriteKindDelete {
			return nil, nil
		}
		iter2.Seek(iter1.Item().Key())
		if iter2.Valid() && bytes.Compare(DecodeUserKey(iter2.Item().Key()), key) == 0 && DecodeTimestamp(iter2.Item().Key()) >= write.StartTS {
			value, err = iter2.Item().Value()
			if err != nil {
				return nil, err
			}
			return value, nil
		}
	}
	return nil, nil
	// Your Code Here (4A).
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{Key: EncodeKey(key, txn.StartTS), Value: value, Cf: engine_util.CfDefault}})
	// Your Code Here (4A).
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{Key: EncodeKey(key, txn.StartTS), Cf: engine_util.CfDefault}})
	// Your Code Here (4A).
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	for iter.Seek(EncodeKey(key, math.MaxUint64)); iter.Valid(); iter.Next() {
		value, err := iter.Item().Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		if bytes.Compare(DecodeUserKey(iter.Item().Key()), key) == 0 && write.StartTS == txn.StartTS {
			return write, DecodeTimestamp(iter.Item().Key()), nil
		}
		if bytes.Compare(DecodeUserKey(iter.Item().Key()), key) > 0 {
			return nil, 0, nil
		}
	}
	// Your Code Here (4A).
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(key, math.MaxUint64))
	if iter.Valid() && bytes.Compare(DecodeUserKey(iter.Item().Key()), key) == 0 {
		value, err := iter.Item().Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		return write, DecodeTimestamp(iter.Item().Key()), nil

	}
	// Your Code Here (4A).
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func DecodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
