package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	key := req.GetKey()
	cf := req.GetCf()
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	value, _ := reader.GetCF(cf, key)
	if value == nil {
		return &kvrpcpb.RawGetResponse{Value: value, NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: value, NotFound: false}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	key := req.GetKey()
	value := req.GetValue()
	cf := req.GetCf()
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: value,
			Cf:    cf,
		},
	})
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, nil
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	key := req.GetKey()
	cf := req.GetCf()
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  cf,
		},
	})
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startkey := req.GetStartKey()
	limit := req.GetLimit()
	cf := req.GetCf()
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	iter := reader.IterCF(cf)
	defer iter.Close()
	var kvs []*kvrpcpb.KvPair
	iter.Seek(startkey)
	for i := uint32(0); i < limit; i++ {
		if iter.Valid() {
			key := iter.Item().Key()
			value, _ := iter.Item().Value()
			kvs = append(kvs, &kvrpcpb.KvPair{Key: key,
				Value: value})
			iter.Next()
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
