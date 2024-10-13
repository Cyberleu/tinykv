package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	StartKey []byte
	Txn      *MvccTxn
	iter3    engine_util.DBIterator
	LastKey  []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	if startKey != nil && txn != nil {
		scanner := &Scanner{StartKey: startKey, Txn: txn}
		scanner.iter3 = txn.Reader.IterCF(engine_util.CfWrite)
		scanner.iter3.Seek(EncodeKey(startKey, TsMax))
		return scanner
	}
	return nil
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter3.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for scan.iter3.Valid() {
		key := DecodeUserKey(scan.iter3.Item().Key())
		ts := DecodeTimestamp(scan.iter3.Item().Key())
		value, err := scan.iter3.Item().Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, nil, err
		}
		if bytes.Compare(scan.LastKey, key) != 0 && ts <= scan.Txn.StartTS && write.Kind == WriteKindPut {
			// key与上一个不同且对当前事务可见且写类型为提交
			value, err := scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
			if err != nil {
				return nil, nil, err
			}
			if value != nil {
				scan.LastKey = key
				scan.iter3.Next()
				return key, value, nil
			}
		} else if bytes.Compare(scan.LastKey, key) != 0 && ts <= scan.Txn.StartTS && write.Kind == WriteKindDelete {
			scan.LastKey = key
		}
		scan.iter3.Next()
	}
	return nil, nil, nil
}
