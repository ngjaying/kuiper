package main

import (
	"github.com/emqx/kuiper/xstream/server/server"
	"log"
	"net/http"
	_ "net/http/pprof"
)

var (
	Version      = "unknown"
	LoadFileType = "relative"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	server.StartUp(Version, LoadFileType)
}
