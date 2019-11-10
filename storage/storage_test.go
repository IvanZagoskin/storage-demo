package storage_test

import (
	"testing"
	"time"

	"github.com/IvanZagoskin/storage-demo/storage"
)

func TestStorage(t *testing.T) {
	testCases := []struct {
		name  string
		items []*storage.Item
	}{
		{
			name: "simple put",
			items: []*storage.Item{
				&storage.Item{Key: "1", Value: "1", TTL: time.Now().Unix() + 15*time.Second.Nanoseconds()},
			},
		},
	}

	storage, err := storage.NewStorage()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := storage.Shutdown(); err != nil {
			t.Error(err)
		}
	}()

	for _, tc := range testCases {
		for i := range tc.items {
			if err := storage.Put(tc.items[i]); err != nil {
				t.Error(err)
			}
		}

		for i := range tc.items {
			value, err := storage.Get(tc.items[i].Key)
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
