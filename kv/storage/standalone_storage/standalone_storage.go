package standalone_storage

import (
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")

	kvDB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(kvDB, nil, kvPath, "")
	return &StandAloneStorage{engines: engines, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engines.Kv.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return s.getReader(), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writebatch := &engine_util.WriteBatch{}
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			writebatch.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			writebatch.DeleteCF(m.Cf(), m.Key())
		}
	}
	return s.engines.WriteKV(writebatch)
}

type reader struct {
	txn *badger.Txn
}

func (s *StandAloneStorage) getReader() *reader {
	return &reader{
		txn: s.engines.Kv.NewTransaction(false),
	}
}

func (r *reader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *reader) Close() {
	r.txn.Discard()
}
