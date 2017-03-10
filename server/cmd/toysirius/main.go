package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"

	"github.com/golang/glog"
	"github.com/jacobwpeng/sirius/engine"
	"github.com/jacobwpeng/sirius/server"
)

var config server.AppConfig

func init() {
	flag.StringVar(&config.AcceptClientAddress, "clientaddr", ":9427",
		"Client listening address")
	flag.StringVar(&config.AcceptServerAddress, "serveraddr", ":9428",
		"Server listening address")
}

func main() {
	go func() {
		glog.Info(http.ListenAndServe(":6060", nil))
	}()
	flag.Parse()
	app := server.NewApp(config)
	app.AddRank(engine.RankEngineConfig{})
	app.Run()
}
