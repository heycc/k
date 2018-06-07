package collector_puller

type Configure struct {
	MaxIdleConnsPerHost int
	MaxPullerWorker     int
	PullRequestTimeout  int
	PullInterval        int
	BufferedChanSize    int
	ExporterPort        int
	MysqlDsn            string
	LogLevel            string
}

type ExporterEndPoint struct {
	Host string
	Uri  string
}
