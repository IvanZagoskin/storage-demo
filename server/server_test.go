package server_test

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"

	"github.com/IvanZagoskin/storage-demo/server"
)

type mockService struct{}

func (m *mockService) Get(key string) (string, error) {
	return key, nil
}

func (m *mockService) Put(key, value string, expiration int64) error {
	return nil
}

func (m *mockService) Delete(key string) error {
	return nil
}

func TestServer(t *testing.T) {
	s := server.NewServer(&mockService{})
	defer s.Shutdown()
	go func() {
		if err := s.ListenAndServe(); err != nil {
			t.Fatal(err)
		}
	}()

	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req, _ := json.Marshal(&server.GetReq{Key: "someKey"})
	sReq := "GET\n" + string(req) + "\n"
	if _, err := conn.Write([]byte(sReq)); err != nil {
		t.Fatal(err)
	}

	msg, _, err := bufio.NewReader(conn).ReadLine()
	if err != nil {
		t.Fatal(err)
	}

	res := &server.GetRes{}
	if err := json.Unmarshal(msg, res); err != nil {
		t.Fatal(err)
	}

	if res.Value != "someKey" || res.Err != nil {
		t.Errorf("unexpected result %+v", res)
	}

}
