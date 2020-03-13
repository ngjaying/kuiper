package main

import (
	"github.com/emqx/kuiper/xstream/server/server"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
)

var Version string = "unknown"

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.SetOutput(os.Stdout)

	runtime.GOMAXPROCS(1)
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)

	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}()

	server.StartUp(Version)
}
