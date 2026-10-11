package metadata

import (
	"encoding/json"

	"github.com/dgraph-io/badger/v4"
)

type ChunkInfo struct {
	ID       string `json:"id"`
	Node     string `json:"node"`
	Checksum string `json:"checksum"`
	Size     int    `json:"size"`
}

type ObjectInfo struct {
	ID     string      `json:"id"`
	Chunks []ChunkInfo `json:"chunks"`
}

type Store struct {
	db *badger.DB
}

func NewStore(path string) (*Store, error) {
	opts := badger.DefaultOptions(path).WithLogger(nil)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) SaveObject(object ObjectInfo) error {
	data, err := json.Marshal(object)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("object:"+object.ID), data)
	})
}

func (s *Store) GetObject(objectID string) (*ObjectInfo, error) {
	var object ObjectInfo

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("object:" + objectID))
		if err != nil {
			return err
		}

		return item.Value(func(data []byte) error {
			return json.Unmarshal(data, &object)
		})
	})

	if err != nil {
		return nil, err
	}

	return &object, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}
