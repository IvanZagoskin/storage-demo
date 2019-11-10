package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"time"
)

var ErrUnexpectedTypeOp = errors.New("unexpected type of operation")

type Service interface {
	Put(key, value string, expirationTime int64) error
	Get(key string) (string, error)
	Delete(key string) error
}

type Server struct {
	listener   net.Listener
	service    Service
	logger     *log.Logger
	shutdownCh chan struct{}
}

func NewServer(service Service) *Server {
	server := Server{
		service: service,
		logger:  log.New(os.Stdout, "", log.LstdFlags),
	}

	return &server
}

func (s *Server) ListenAndServe() error {
	var err error
	s.listener, err = net.Listen("tcp", ":8080")
	if err != nil {
		return err
	}
	s.logger.Println("start tcp server on port :8080")
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// s.logger.Println(err)
			continue
		}
		s.logger.Println("accept conn ", conn.RemoteAddr())

		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			s.logger.Println(err)
		}

		go s.handle(conn)
	}
}

func (s *Server) Shutdown() error {
	if err := s.listener.Close(); err != nil {
		s.logger.Println(err)
		return err
	}
	return nil
}

type typeMsg string

const (
	get    typeMsg = "GET"
	put            = "PUT"
	delete         = "DELETE"
)

type GetReq struct {
	Key string
}

type GetRes struct {
	Value string
	Err   error
}

type DeleteReq struct {
	Key string
}

type DeleteRes struct {
	Err error
}

type PutReq struct {
	Key        string
	Value      string
	Expiration int64
}

type PutRes struct {
	Err error
}

type UnexpectedRes struct {
	Err error
}

func (s *Server) handle(conn net.Conn) {
	s.logger.Println("handle conn ", conn.RemoteAddr())
	wConn := bufio.NewWriter(conn)
	scanner := bufio.NewScanner(conn)

	for {
		if !scanner.Scan() {
			// s.logger.Println(scanner.Err())
			if scanner.Err() != io.EOF {
				continue
			}
			s.logger.Println("connection closed", conn.RemoteAddr())
			conn.Close()
			return
		}

		switch typeMsg(scanner.Text()) {
		case get:
			if scanner.Scan() {
				req := &GetReq{}
				if err := json.Unmarshal(scanner.Bytes(), req); err != nil {
					s.logger.Println(err)
				}

				value, err := s.service.Get(req.Key)
				res, err := json.Marshal(&GetRes{Err: err, Value: value})
				if err != nil {
					s.logger.Println(err)
				}

				if _, err := wConn.Write(append(res, []byte("\n")...)); err != nil {
					s.logger.Println(err)
				}
				wConn.Flush()
			}

		case put:
			if scanner.Scan() {
				msg := &PutReq{}
				if err := json.Unmarshal(scanner.Bytes(), msg); err != nil {
					s.logger.Println(err)
				}

				err := s.service.Put(msg.Key, msg.Value, msg.Expiration)
				res, err := json.Marshal(&PutRes{Err: err})
				if err != nil {
					s.logger.Println(err)
				}

				if _, err := wConn.Write(append(res, []byte("\n")...)); err != nil {
					s.logger.Println(err)
				}
				wConn.Flush()
			}

		case delete:
			if scanner.Scan() {
				msg := &DeleteReq{}
				if err := json.Unmarshal(scanner.Bytes(), msg); err != nil {
					s.logger.Println(err)
				}

				err := s.service.Delete(msg.Key)
				res, err := json.Marshal(&DeleteRes{Err: err})
				if err != nil {
					s.logger.Println(err)
				}

				if _, err := wConn.Write(append(res, []byte("\n")...)); err != nil {
					s.logger.Println(err)
				}
				wConn.Flush()
			}
		default:
			s.logger.Println("unexpected type of operation")
			res, err := json.Marshal(&UnexpectedRes{Err: ErrUnexpectedTypeOp})
			if err != nil {
				s.logger.Println(err)
			}

			if _, err := wConn.Write(append(res, []byte("\n")...)); err != nil {
				s.logger.Println(err)
			}
			wConn.Flush()
		}
	}
}
