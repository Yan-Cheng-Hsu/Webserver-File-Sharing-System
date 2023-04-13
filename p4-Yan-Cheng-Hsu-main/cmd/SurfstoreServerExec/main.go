package main

import (
	"cse224/proj4/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {})
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	outputLog := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	args := flag.Args()
	blockStoreAddr := ""
	if len(args) == 1 {
		blockStoreAddr = args[0]
	}
	_, ok := SERVICE_TYPES[strings.ToLower(*service)]
	if !ok {
		flag.Usage()
		fmt.Println("Input format wrong!")
		os.Exit(EX_USAGE)
	}
	metaStoreAddr := ":" + strconv.Itoa(*port)
	if *localOnly {
		metaStoreAddr = "localhost" + metaStoreAddr
	}

	// Disable log outputs if debug flag is missing
	if !(*outputLog) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	serverType := strings.ToLower(*service)
	if serverType == "meta" {
		startMeta(metaStoreAddr, blockStoreAddr)
	} else if serverType == "block" {
		startBlock(metaStoreAddr)
	} else {
		if metaStoreAddr == blockStoreAddr {
			listener, _ := net.Listen("tcp", metaStoreAddr)
			metaStore := surfstore.NewMetaStore(blockStoreAddr)
			blockStore := surfstore.NewBlockStore()
			s := grpc.NewServer()
			surfstore.RegisterMetaStoreServer(s, metaStore)
			surfstore.RegisterBlockStoreServer(s, blockStore)
			s.Serve(listener)
		} else {
			go startMeta(metaStoreAddr, blockStoreAddr)
			startBlock(blockStoreAddr)
		}
	}
}

func startMeta(hostAddr string, blockStoreAddr string) {
	listener, _ := net.Listen("tcp", hostAddr)
	metaStore := surfstore.NewMetaStore(blockStoreAddr)
	s := grpc.NewServer()
	surfstore.RegisterMetaStoreServer(s, metaStore)
	s.Serve(listener)
}
func startBlock(blockStoreAddr string) {
	listener, _ := net.Listen("tcp", blockStoreAddr)
	blockStore := surfstore.NewBlockStore()
	s := grpc.NewServer()
	surfstore.RegisterBlockStoreServer(s, blockStore)
	s.Serve(listener)
}
