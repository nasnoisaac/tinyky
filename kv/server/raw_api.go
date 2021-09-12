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
	s := server.storage
	cf := req.Cf
	key := req.Key
	res := kvrpcpb.RawGetResponse{}

	reader, err := s.Reader(req.Context)
	if err != nil {
		return &res, err
	}
	res.Value, err = reader.GetCF(cf, key)
	if res.Value == nil {
		res.NotFound = true
	}
	reader.Close()
	return &res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	s := server.storage
	cf := req.Cf
	key := req.Key
	val := req.Value
	res := kvrpcpb.RawPutResponse{}

	data := storage.Put{
		Key:   key,
		Value: val,
		Cf:    cf,
	}
	err := s.Write(req.Context, []storage.Modify{{data}})
	return &res, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	s := server.storage
	cf := req.Cf
	key := req.Key
	res := kvrpcpb.RawDeleteResponse{}

	data := storage.Delete{
		Key: key,
		Cf:  cf,
	}
	err := s.Write(req.Context, []storage.Modify{{data}})
	return &res, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	s := server.storage
	cf := req.Cf
	start_key := req.StartKey
	limit := req.Limit
	res := kvrpcpb.RawScanResponse{}

	reader, err := s.Reader(req.Context)
	it := reader.IterCF(cf)
	it.Seek(start_key)

	for i := uint32(0); i < limit; i++ {
		print(i)
		item := it.Item()
		key := item.Key()
		val, _ := item.Value()
		kv := kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		res.Kvs = append(res.Kvs, &kv)
		it.Next()
		if !it.Valid() {
			break
		}
	}
	it.Close()
	return &res, err
}
