package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNilItem     = errors.New("item can't be nil")
	ErrKeyNotFound = errors.New("key not found")
)

// Item of storage
type Item struct {
	Key        string
	Expiration int64
	Value      string
}

// Storage is a thread-safety key/value storage
type Storage struct {
	mu         sync.RWMutex
	data       map[string]*Item
	shutdownCh chan struct{}
	pathToBak  string
}

// NewStorage returns storage and starts job to delete expired items
func NewStorage(pathToBak string, jobInterval time.Duration) (*Storage, error) {
	storage := &Storage{
		data:      make(map[string]*Item, 0),
		pathToBak: pathToBak,
	}
	go storage.runTTLJob(jobInterval)

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

func (s *Storage) runTTLJob(jobInterval time.Duration) {
	ticker := time.NewTicker(jobInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			now := time.Now().Unix()
			expiredKeys := make([]string, 0)
			s.mu.RLock()
			for key, val := range s.data {
				if val.Expiration <= now {
					expiredKeys = append(expiredKeys, key)
				}
			}
			s.mu.RUnlock()

			for i := range expiredKeys {
				s.mu.Lock()
				delete(s.data, expiredKeys[i])
				s.mu.Unlock()
			}
		}
	}
}

// Put is a method that adds or updates a value by key, and also sets the lifetime
func (s *Storage) Put(key, value string, expirationTime int64) error {
	s.mu.RLock()
	s.data[key] = &Item{Key: key, Value: value, Expiration: expirationTime}
	s.mu.RUnlock()
	return nil
}

// Get returns value by key
func (s *Storage) Get(key string) (string, error) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return "", ErrKeyNotFound
	}
	return v.Value, nil
}

// Delete deletes item from storage by key
func (s *Storage) Delete(key string) {
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

func (s *Storage) reset() {
	s.mu.Lock()
	s.mu.Unlock()
}

// Backup is a method that backup storage data to writer
func (s *Storage) Backup(writer io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
		s.Put(item.Key, item.Value, item.Expiration)
	}

	if err := scaner.Err(); err != nil {
		return err
	}

	return nil
}

// Shutdown is a method that stops starts job to delete expired items and backups data
func (s *Storage) Shutdown() error {
	_, err := os.Stat(s.pathToBak)
	if !os.IsNotExist(err) {
		if err := os.Rename(s.pathToBak, strconv.Itoa(int(time.Now().Unix()))+"_"+s.pathToBak); err != nil {
			return err
		}
	}

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
