package storage

import (
	"time"

	"github.com/dgraph-io/badger"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/andrew-delph/my-key-store/config"
)

func Value() string {
	c := config.GetConfig()
	badgerTest()
	leveldbTest()
	cacheTest()
	NewLevelDbStorage(c.Storage)

	logrus.Warn(">>> ", c.Manager.DataPath)

	return "test"
}

func badgerTest() {
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	defer db.Close()
	if err != nil {
		logrus.Fatal(err)
	}
}

func leveldbTest() {
	db, err := leveldb.OpenFile("/tmp/level", nil)
	defer db.Close()
	if err != nil {
		logrus.Fatal(err)
	}
}

func cacheTest() {
	cache.New(0*time.Minute, 1*time.Minute)
}

type Store interface {
	WriteValue(key []byte, value []byte) error
	ReadValue(key []byte) ([]byte, bool, error)
	Iterate(Start []byte, Limit []byte) Iterator
}

type Iterator interface {
	First() bool
	Next() bool
	isDone() bool
	Key() []byte
	Value() []byte
	Release()
}
