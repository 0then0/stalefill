package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0then0/stalefill/internal/demo"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	redis := flag.String("redis", "127.0.0.1:6380", "Redis proxy address")
	mode := flag.String("mode", "broken", "broken or fixed")
	flag.Parse()
	if *mode != "broken" && *mode != "fixed" {
		fmt.Fprintln(os.Stderr, "mode must be broken or fixed")
		os.Exit(2)
	}
	l, e := net.Listen("tcp", *addr)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(2)
	}
	server := &http.Server{Handler: demo.New(*redis, *mode == "fixed"), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
		server.Close()
	}()
	fmt.Printf("Demo %s listening on %s; Redis %s\n", *mode, l.Addr(), *redis)
	if e = server.Serve(l); e != nil && e != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(2)
	}
}
