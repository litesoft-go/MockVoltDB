package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/litesoft-go/mockvoltdb/pkg/canned"
	"github.com/litesoft-go/mockvoltdb/pkg/utils"
	"github.com/litesoft-go/mockvoltdb/version"
)

const PORT_0_http_____ = 8080
const PORT_1_internal_ = 3021
const PORT_2_rep______ = 5555
const PORT_3_zk_______ = 7181
const PORT_4_jmx______ = 9090
const PORT_5_admin____ = 21211
const PORT_6_client___ = 21212
const PORT_7_status___ = 11780

var responses = make(map[string]*utils.Responder)

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
	query := url.RawQuery
	urlPathQuery := url.Path
	if query != "" {
		urlPathQuery += "?" + query
	}
	resp := responses[port].GetResponse(urlPathQuery)
	cType := "application/json"
	body := resp.GetBody()
	status := resp.GetStatus()
	msg := fmt.Sprintf("Request: %s%s%s%s", ourPrivateClassA, host, port, urlPathQuery)
	if status == 404 {
		msg = "**** 404 - " + msg
		body = "port[" + port + "] " + body
		cType = "text/plain"
	}
	fmt.Println(msg)
	w.Header().Set("Content-Type", cType)
	w.WriteHeader(status)
	count, err := fmt.Fprint(w, body)
	if err != nil {
		fmt.Printf("******************** type '%s' %d, bytes: %d, error: %v\n", cType, status, count, err)
	}
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
		responses[":11780"] = canned.PortResonses_status() // PORT_7_status___
		responses[":8080"] = canned.PortResonses_http()    // PORT_0_http_____

		http.HandleFunc("/", handler)
		err = doAllWork(
			PORT_0_http_____,
			PORT_1_internal_,
			PORT_2_rep______,
			PORT_3_zk_______,
			PORT_4_jmx______,
			PORT_5_admin____,
			PORT_6_client___,
			PORT_7_status___,
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
