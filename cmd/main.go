package main

import (
	"fmt"
	"log"
	"net/http"

	utils "github.com/litesoft-go/mockVoltdb/pkg/utils"
)

func handler(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	url := r.URL
	//host := url.Host
	port := url.Port()
	_, _ = fmt.Fprintf(w, "Hi there, I love %s (from %s,%s)!",
		url.Path[1:], host, port)
}

func main() {

	ipv4s, err := IPv4s()
	//net.InterfaceAddrs()

	fmt.Printf("(IPv4s):\n")
	for _, ipv4 := range ipv4s {
		//if ip != nil {
		fmt.Printf("   %s\n", ipv4.String())
		//}
	}
	if err == nil {
		http.HandleFunc("/", handler)
		err = http.ListenAndServe(":8080", nil)
	}
	log.Fatal(err)
}
