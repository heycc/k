package main

import (
	"github.com/koding/multiconfig"
	"k/collector_puller"
	"log"
	"net/http"
	_ "net/http/pprof"
	//"os/signal"
	//"syscall"
	"time"
)

var (
	config      *collector_puller.Configure
	lg          *collector_puller.Logger
	logLevelMap map[string]int
)

func main() {
	config = new(collector_puller.Configure)
	c := multiconfig.NewWithPath("puller.toml")
	c.MustLoad(config)

	lg = &collector_puller.Logger{}
	lg.Init("puller.log", config.LogLevel)

	taskChan := make(chan collector_puller.ExporterEndPoint, config.BufferedChanSize)

	ts := &http.Transport{
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
	}
	client := &http.Client{
		Transport: ts,
		Timeout:   time.Duration(config.PullRequestTimeout) * time.Second,
	}

	for i := 0; i < config.MaxPullerWorker; i = i + 1 {
		worker := &collector_puller.PullerWorker{
			MysqlDsn:     config.MysqlDsn,
			Logger:       lg,
			ExporterPort: config.ExporterPort,
			TaskChan:     taskChan,
			Client:       client,
			WokerId:      i,
		}
		go func() {
			worker.Start()
		}()
	}

	tasker := &collector_puller.Tasker{
		MysqlDsn: config.MysqlDsn,
		Logger:   lg,
		TaskChan: taskChan,
	}
	go func() {
		tasker.Start()
	}()

	log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
}
