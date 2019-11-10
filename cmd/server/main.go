package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IvanZagoskin/storage-demo/server"

	"github.com/IvanZagoskin/storage-demo/service"

	"github.com/IvanZagoskin/storage-demo/storage"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	stg, err := storage.NewStorage("", 2*time.Second)
	if err != nil {
		logger.Println(err)
		os.Exit(1)
	}

	srv := server.NewServer(service.NewService(stg))
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			logger.Println(err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	<-c
	stg.Shutdown()
	srv.Shutdown()
}
