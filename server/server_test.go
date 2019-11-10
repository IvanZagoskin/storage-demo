package server_test

import (
	"bufio"
	"encoding/json"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/IvanZagoskin/storage-demo/service"

	"github.com/IvanZagoskin/storage-demo/storage"

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

	if res.Value != "someKey" || res.Err != "" {
		t.Errorf("unexpected result %+v", res)
	}
}

const newLine string = "\n"

func TestServerOperations(t *testing.T) {
	stg, err := storage.NewStorage("", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	src := service.NewService(stg)
	srv := server.NewServer(src)
	defer func() {
		if err := srv.Shutdown(); err != nil {
			t.Fatal(err)
		}
		if err := stg.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			t.Fatal(err)
		}
	}()

	testCases := []struct {
		name          string
		typeOperation string
		data          interface{}
		expectedRes   interface{}
	}{
		{
			name:          "simple put",
			typeOperation: "PUT",
			data:          &server.PutReq{Value: "1", Key: "1", Expiration: time.Now().Add(time.Minute).Unix()},
			expectedRes:   &server.PutRes{Err: ""},
		},
		{
			name:          "simple get",
			typeOperation: "GET",
			data:          &server.GetReq{Key: "1"},
			expectedRes:   &server.GetRes{Value: "1", Err: ""},
		},
		{
			name:          "simple delete",
			typeOperation: "DELETE",
			data:          &server.DeleteReq{Key: "1"},
			expectedRes:   &server.DeleteRes{Err: ""},
		},
		{
			name:          "simple delete(2)",
			typeOperation: "DELETE",
			data:          &server.DeleteReq{Key: "2"},
			expectedRes:   &server.DeleteRes{Err: storage.ErrKeyNotFound.Error()},
		},
		{
			name:          "simple delete",
			typeOperation: "STRANGERTHING",
			data:          &server.DeleteReq{Key: "1"},
			expectedRes:   &server.UnexpectedRes{Err: server.ErrUnexpectedTypeOp.Error()},
		},
	}

	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, tc := range testCases {
		data, err := json.Marshal(tc.data)
		if err != nil {
			t.Fatalf("failed %s.\nerr: %s", tc.name, err)
		}

		req := []byte(tc.typeOperation + newLine + string(data) + newLine)
		if _, err := conn.Write(req); err != nil {
			t.Fatalf("failed %s.\nerr: %s", tc.name, err)
		}

		resBody, _, err := bufio.NewReader(conn).ReadLine()
		if err != nil {
			t.Fatalf("failed %s.\nerr: %s", tc.name, err)
		}

		var response interface{}
		switch tc.typeOperation {
		case "PUT":
			response = &server.PutRes{}
		case "GET":
			response = &server.GetRes{}
		case "DELETE":
			response = &server.DeleteRes{}
		default:
			response = &server.UnexpectedRes{}
		}
		if err := json.Unmarshal(resBody, response); err != nil {
			t.Fatalf("failed %s.\nerr: %s", tc.name, err)
		}

		if !reflect.DeepEqual(response, tc.expectedRes) {
			t.Fatalf("failed %s\ngot: %+v\nexpected: %+v", tc.name, response, tc.expectedRes)
		}
	}
}
