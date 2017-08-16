package manager

import (
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/zero-os/0-stor/server/db"
)

type StoreStatManager struct {
	db db.DB
	mu sync.RWMutex
}

func NewStoreStatMgr(db db.DB) *StoreStatManager {
	return &StoreStatManager{
		db: db,
		mu: sync.RWMutex{},
	}
}

func (s *StoreStatManager) Get() (db.StoreStat, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	storeStat := db.StoreStat{}
	b, err := s.db.Get([]byte(STORE_STATS_PREFIX))
	if err != nil {
		log.Errorln(err.Error())
		return storeStat, err
	}

	if err := storeStat.Decode(b); err != nil {
		log.Errorln(err.Error())
		return storeStat, err
	}
	return storeStat, nil
}

func (s *StoreStatManager) Set(available, used uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	storeStat := db.StoreStat{
		SizeAvailable: available,
		SizeUsed:      used,
	}

	b, err := storeStat.Encode()
	if err != nil {
		return err
	}

	return s.db.Set([]byte(STORE_STATS_PREFIX), b)
}
