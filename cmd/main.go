package main

import (
	"context"
	"fmt"
	"github.com/litesoft-go/mockvoltdb/pkg/utils"
	"github.com/litesoft-go/mockvoltdb/version"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

const PORT_0_http___ = 8080
const PORT_1_int____ = 3021
const PORT_2_rep____ = 5555
const PORT_3_zk_____ = 7181
const PORT_4_jmx____ = 9090
const PORT_5_admin__ = 21211
const PORT_6_client_ = 21212
const PORT_7_status_ = 11780

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
	msg := fmt.Sprintf("Request: %s%s%s%s", ourPrivateClassA, host, port, url.Path)
	fmt.Println(msg)

	_, _ = fmt.Fprint(w, msg)
}

var ourPrivateClassA = ""

func main() {
	fmt.Printf("MockVoltDB Version: %s\n", version.Version)
	fmt.Printf("Args: %v\n", os.Args[1:])

	ipv4s, err := utils.IPv4s()

	fmt.Println("(IPv4s):")
	for _, ipv4 := range ipv4s {
		fmt.Printf("   %s\n", ipv4.String())
		if ipv4.IsPrivateClassA() {
			ourPrivateClassA = ipv4.String() + "|"
		}
	}
	if err == nil {
		http.HandleFunc("/", handler)
		err = doAllWork(
			PORT_0_http___,
			PORT_1_int____,
			PORT_2_rep____,
			PORT_3_zk_____,
			PORT_4_jmx____,
			PORT_5_admin__,
			PORT_6_client_,
			PORT_7_status_,
		)
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
