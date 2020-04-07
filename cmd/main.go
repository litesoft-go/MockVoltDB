package main

import (
	"context"
	"fmt"
	"github.com/litesoft-go/mockvoltdb/pkg/utils"
	"github.com/litesoft-go/mockvoltdb/version"
	"log"
	"net/http"
	"strings"
	"sync"
)

func splitHost(pHost string) (rHost string, rPort string) {
	rHost = pHost
	if at := strings.IndexByte(pHost, ':'); at != -1 {
		rHost = pHost[:at]
		rPort = pHost[at:]
	}
	return
}

func handler(w http.ResponseWriter, r *http.Request) {
	host, port := splitHost(r.Host)
	url := r.URL
	msg := fmt.Sprintf("Request: %s%s%s", host, port, url.Path)
	fmt.Println(msg)

	_, _ = fmt.Fprint(w, msg)
}

func main() {
	fmt.Printf("MockVoltDB Version: %s\n", version.Version)

	ipv4s, err := utils.IPv4s()

	fmt.Println("(IPv4s):")
	for _, ipv4 := range ipv4s {
		//if ip != nil {
		fmt.Printf("   %s\n", ipv4.String())
		//}
	}
	if err == nil {
		http.HandleFunc("/", handler)
		err = doAllWork(8080, 8081)
		// err = http.ListenAndServe(":8080", nil)
	}
	log.Fatal(err)
}

func doAllWork(ports ...int) error {
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Make sure it's called to release resources even if no errors

	for _, port := range ports {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()

			err := work(port)
			if err != nil {
				fmt.Printf("Worker #%d, error: %v\n", port, err)
				cancel()
				return
			}
		}(port)
	}
	wg.Wait()

	return ctx.Err()
}

func work(port int) error {
	fmt.Printf("Listening on %d\n", port)
	addr := fmt.Sprintf(":%d", port)
	err := http.ListenAndServe(addr, nil)
	if err != http.ErrServerClosed {
		return err
	}
	return nil
}
