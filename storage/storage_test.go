package storage_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/IvanZagoskin/storage-demo/storage"
)

func TestPut(t *testing.T) {
	testCases := []struct {
		name  string
		items []*storage.Item
	}{
		{
			name: "simple put",
			items: []*storage.Item{
				&storage.Item{Key: "1", Value: "1", Expiration: time.Now().Add(15 * time.Second).Unix()},
			},
		},
	}

	s, err := storage.NewStorage("storage.bak", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Shutdown(); err != nil {
			t.Error(err)
		}
	}()

	for _, tc := range testCases {
		for i := range tc.items {
			if err := s.Put(tc.items[i].Key, tc.items[i].Value, tc.items[i].Expiration); err != nil {
				t.Error(err)
			}
		}

		for i := range tc.items {
			value, err := s.Get(tc.items[i].Key)
			if err != nil {
				t.Error(err)
				continue
			}

			if value != tc.items[i].Value {
				t.Errorf("%s failed.\ngot:%v\nexpected:%v\n", tc.name, value, tc.items[i].Value)
			}
		}
	}
}

func TestExpiration(t *testing.T) {
	testCases := []struct {
		name  string
		items []*storage.Item
	}{
		{
			name: "simple expiration check",
			items: []*storage.Item{
				&storage.Item{Key: "1", Value: "1", Expiration: time.Now().Unix()},
				&storage.Item{Key: "2", Value: "2", Expiration: time.Now().Unix()},
				&storage.Item{Key: "3", Value: "3", Expiration: time.Now().Unix()},
				&storage.Item{Key: "4", Value: "4", Expiration: time.Now().Unix()},
				&storage.Item{Key: "5", Value: "5", Expiration: time.Now().Unix()},
			},
		},
	}

	jobInterval := 2 * time.Second
	s, err := storage.NewStorage("storage.bak", jobInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Shutdown(); err != nil {
			t.Error(err)
		}
	}()

	for _, tc := range testCases {
		for i := range tc.items {
			if err := s.Put(tc.items[i].Key, tc.items[i].Value, tc.items[i].Expiration); err != nil {
				t.Error(err)
			}
		}

		time.Sleep(jobInterval + time.Second)

		for i := range tc.items {
			_, err := s.Get(tc.items[i].Key)
			if err != storage.ErrKeyNotFound {
				t.Errorf("%s failed. unexpected err: %v", tc.name, err)
			}
		}
	}
}

func TestDelete(t *testing.T) {
	testCases := []struct {
		name  string
		items []*storage.Item
	}{
		{
			name: "simple delete",
			items: []*storage.Item{
				&storage.Item{Key: "1", Value: "1", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "2", Value: "2", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "3", Value: "3", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "4", Value: "4", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "5", Value: "5", Expiration: time.Now().Add(time.Minute).Unix()},
			},
		},
	}

	jobInterval := 2 * time.Second
	s, err := storage.NewStorage("storage.bak", jobInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Shutdown(); err != nil {
			t.Error(err)
		}
	}()

	for _, tc := range testCases {
		for i := range tc.items {
			if err := s.Put(tc.items[i].Key, tc.items[i].Value, tc.items[i].Expiration); err != nil {
				t.Error(err)
			}
		}

		for i := range tc.items {
			s.Delete(tc.items[i].Key)
			_, err := s.Get(tc.items[i].Key)
			if err != storage.ErrKeyNotFound {
				t.Errorf("%s failed. unexpected err: %v", tc.name, err)
			}
		}
	}
}

func TestBackupAndRestore(t *testing.T) {
	testCases := []struct {
		name  string
		items []*storage.Item
	}{
		{
			name: "simple backup and restore",
			items: []*storage.Item{
				&storage.Item{Key: "1", Value: "1", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "2", Value: "2", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "3", Value: "3", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "4", Value: "4", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "5", Value: "5", Expiration: time.Now().Add(time.Minute).Unix()},
			},
		},
	}

	jobInterval := 2 * time.Second
	s, err := storage.NewStorage("storage.bak", jobInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Shutdown(); err != nil {
			t.Error(err)
		}
	}()

	buf := bytes.NewBuffer([]byte{})
	for _, tc := range testCases {
		buf.Reset()
		for i := range tc.items {
			data, err := json.Marshal(tc.items[i])
			if err != nil {
				t.Fatal(err)
			}

			if _, err := buf.Write(append(data, []byte("\n")...)); err != nil {
				t.Fatal(err)
			}
		}

		if err := s.Restore(buf); err != nil {
			t.Fatal(err)
		}

		for i := range tc.items {
			value, err := s.Get(tc.items[i].Key)
			if err != nil {
				t.Error(err)
				continue
			}

			if value != tc.items[i].Value {
				t.Errorf("%s failed.\ngot:%v\nexpected:%v\n", tc.name, value, tc.items[i].Value)
			}
		}

		buf.Reset()
		if err := s.Backup(buf); err != nil {
			t.Error(err)
		}

		scanner := bufio.NewScanner(buf)
		for scanner.Scan() {
			got := &storage.Item{}
			if err := json.Unmarshal(scanner.Bytes(), got); err != nil {
				t.Fatal(err)
			}

			isContains := false
			for i := range tc.items {
				if *tc.items[i] == *got {
					isContains = true
				}
			}

			if !isContains {
				t.Errorf("%s failed. not found item: %+v", tc.name, got)
			}

		}
	}
}

func TestRestore(t *testing.T) {
	testCases := []struct {
		name  string
		items []*storage.Item
	}{
		{
			name: "simple expiration check",
			items: []*storage.Item{
				&storage.Item{Key: "1", Value: "1", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "2", Value: "2", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "3", Value: "3", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "4", Value: "4", Expiration: time.Now().Add(time.Minute).Unix()},
				&storage.Item{Key: "5", Value: "5", Expiration: time.Now().Add(time.Minute).Unix()},
			},
		},
	}

	jobInterval := 2 * time.Second
	s, err := storage.NewStorage("storage.bak", jobInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Shutdown(); err != nil {
			t.Error(err)
		}
	}()
	buf := bytes.NewBuffer([]byte{})
	for _, tc := range testCases {
		buf.Reset()
		for i := range tc.items {
			data, err := json.Marshal(tc.items[i])
			if err != nil {
				t.Fatal(err)
			}

			if _, err := buf.Write(append(data, []byte("\n")...)); err != nil {
				t.Fatal(err)
			}
		}

		if err := s.Restore(buf); err != nil {
			t.Fatal(err)
		}

		for i := range tc.items {
			value, err := s.Get(tc.items[i].Key)
			if err != nil {
				t.Error(err)
				continue
			}

			if value != tc.items[i].Value {
				t.Errorf("%s failed.\ngot:%v\nexpected:%v\n", tc.name, value, tc.items[i].Value)
			}
		}
	}
}
