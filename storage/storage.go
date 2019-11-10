package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

var (
	errNilItem     = errors.New("item can't be nil")
	errKeyNotFound = errors.New("key not found")
)

// Key of storage
type Key string

// Value if storage
type Value string

// Item of storage
type Item struct {
	Key   Key
	TTL   int64
	Value Value
}

// Storage is a thread-safety key/value storage
type Storage struct {
	sync.RWMutex
	data       map[Key]*Item
	shutdownCh chan struct{}
	pathToBak  string
}

// NewStorage returns storage and starts job to delete expired items
func NewStorage() (*Storage, error) {
	storage := &Storage{
		data:      make(map[Key]*Item, 0),
		pathToBak: "storage.bak",
	}
	go storage.runTTLJob()

	file, err := os.OpenFile(storage.pathToBak, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	err = storage.Restore(file)
	if err != nil {
		return nil, err
	}
	return storage, nil
}

func (s *Storage) runTTLJob() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			now := time.Now().Unix()
			expiredKeys := make([]Key, 0)
			s.RLock()
			for key, val := range s.data {
				if val.TTL >= now {
					expiredKeys = append(expiredKeys, key)
				}
			}
			s.RUnlock()

			for i := range expiredKeys {
				s.Lock()
				delete(s.data, expiredKeys[i])
				s.Unlock()
			}
		}
	}
}

// Put is a method that adds or updates a value by key, and also sets the lifetime
func (s *Storage) Put(item *Item) error {
	if item == nil {
		return errNilItem
	}
	s.RLock()
	s.data[item.Key] = item
	s.RUnlock()
	return nil
}

// Get returns value by key
func (s *Storage) Get(key Key) (Value, error) {
	s.RLock()
	v, ok := s.data[key]
	s.RUnlock()
	if !ok {
		return Value(""), errKeyNotFound
	}
	return v.Value, nil
}

// Delete deletes item from storage by key
func (s *Storage) Delete(key Key) {
	s.Lock()
	delete(s.data, key)
	s.Unlock()
}

// Backup is a method that backup storage data to writer
func (s *Storage) Backup(writer io.Writer) error {
	s.RLock()
	defer s.RUnlock()
	var buf []byte
	var err error
	for _, val := range s.data {
		buf, err = json.Marshal(val)
		if err != nil {
			return err
		}

		if _, err := writer.Write(append(buf, []byte("\n")...)); err != nil {
			return err
		}
	}
	return nil
}

// Restore is a method that restores storage data from reader
func (s *Storage) Restore(reader io.Reader) error {
	scaner := bufio.NewScanner(reader)
	for scaner.Scan() {
		item := &Item{}
		if err := json.Unmarshal(scaner.Bytes(), item); err != nil {
			return err
		}
		s.Put(item)
	}

	if err := scaner.Err(); err != nil {
		return err
	}

	return nil
}

// Shutdown is a method that stops starts job to delete expired items and backups data
func (s *Storage) Shutdown() error {
	file, err := os.OpenFile(s.pathToBak, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := s.Backup(file); err != nil {
		return err
	}

	return nil
}
