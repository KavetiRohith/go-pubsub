package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"
)

var addr = flag.String("addr", ":3000", "http service address")
var subscriptionsFile = flag.String(
	"subscriptions_file",
	"client_subscriptions.json",
	"name of the file containing subscriptions in json",
)
var redisServerAddr = flag.String("redis_server_address", ":6379", "")
var pubsubDataDirPath = flag.String("pubsub_data_dir_path", ".", "")

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)

	flag.Parse()

	redisPool := NewRedisPool(*redisServerAddr)

	subscriptions, err := ReadSubscriptionsFromFile(
		path.Join(*pubsubDataDirPath, *subscriptionsFile),
	)
	if err != nil {
		log.Fatal(err)
	}

	exitChan := make(chan struct{})

	hub := newHub(subscriptions, exitChan)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		hub.run()
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(redisPool, hub, w, r, wg)
	})

	mux.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		servePublish(hub, w, r)
	})

	srv := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("listen: %s\n", err)
		}
	}()
	log.Print("Server Started")

	<-done
	close(exitChan)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer func() {
		// extra handling here
		redisPool.Close()
		cancel()
	}()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server Shutdown Failed:%+v", err)
		return
	}
	log.Println("Waiting for Connections to close")
	wg.Wait()
	log.Println("Server Exited Properly")
}
