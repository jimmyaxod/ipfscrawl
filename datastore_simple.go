package main

import (
	"fmt"

	proto "github.com/gogo/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	datastore_query "github.com/ipfs/go-datastore/query"
	pb "github.com/libp2p/go-libp2p-record/pb"
)

type EasyDatastore struct {
	datastore  datastore.Datastore
	logqueries Outputdata
}

type EasyDatastoreBatch struct {
	ds *EasyDatastore
}

func (edb EasyDatastoreBatch) Commit() error {
	fmt.Println("Commit called")
	return nil
}

func (ed EasyDatastoreBatch) Put(key datastore.Key, value []byte) error {
	return ed.ds.Put(key, value)
}

func (ed EasyDatastoreBatch) Delete(key datastore.Key) error {
	return ed.ds.Delete(key)
}

func NewDatastore() *EasyDatastore {
	return &EasyDatastore{
		datastore:  datastore.NewMapDatastore(),
		logqueries: NewOutputdata("queries", 60*60),
	}
}

func (ed *EasyDatastore) Batch() (datastore.Batch, error) {
	fmt.Println("DS Call to Batch()")
	batch := EasyDatastoreBatch{
		ds: ed,
	}
	return batch, nil
}

func (ed *EasyDatastore) Sync(prefix datastore.Key) error {
	fmt.Printf("DS Call to Sync(%v)\n", prefix)
	return ed.datastore.Sync(prefix)
}

func (ed *EasyDatastore) Close() error {
	fmt.Printf("DS Call to Close()\n")
	return ed.datastore.Close()
}

func (ed *EasyDatastore) Put(key datastore.Key, value []byte) error {
	var rec pb.Record
	proto.Unmarshal(value, &rec)

	fmt.Printf("DS Call to Put(%v %s) %v\n", key, value, rec)
	return ed.datastore.Put(key, value)
}

func (ed *EasyDatastore) Delete(key datastore.Key) error {
	fmt.Printf("DS Call to Delete(%v)\n", key)
	return ed.datastore.Delete(key)
}

func (ed *EasyDatastore) Get(key datastore.Key) ([]byte, error) {
	val, err := ed.datastore.Get(key)
	var rec pb.Record
	proto.Unmarshal(val, &rec)
	fmt.Printf("DS Call to Get(%v)=%v,%v\n", key, rec, err)
	return val, err
}

func (ed *EasyDatastore) Has(key datastore.Key) (bool, error) {
	fmt.Printf("Call to Has(%v)\n", key)
	ok, err := ed.datastore.Has(key)
	return ok, err
}

func (ed *EasyDatastore) GetSize(key datastore.Key) (int, error) {
	fmt.Printf("Call to GetSize(%v)\n", key)
	size, err := ed.datastore.GetSize(key)
	return size, err
}

func (ed *EasyDatastore) Query(query datastore_query.Query) (datastore_query.Results, error) {
	fmt.Printf("Call to Query(%v)\n", query)

	// Get the CID out of the query...
	var cids string
	fmt.Sscanf(query.Prefix, "/providers/%s", &cids)

	fmt.Printf("CID is %s\n", cids)

	cid, err := cid.Decode(cids)
	if err == nil {
		mh := cid.Hash()

		s := fmt.Sprintf("%s,%d,%d,%d,%d,%s", query.Prefix,
			cid.Prefix().Codec, cid.Prefix().Version, cid.Prefix().MhType, cid.Prefix().MhLength, mh.HexString())
		ed.logqueries.WriteData(s)
	} else {
		fmt.Printf("ERROR %v\n", err)
	}
	res, err := ed.datastore.Query(query)
	return res, err
}
