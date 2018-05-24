package multiExporter

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"k/multi_mysqld_exporter/collector"
	"github.com/labstack/echo"
	"log"
	"net/http"
	"strconv"
)

type MultiExporter struct {
	dbPool map[int]*sql.DB
	user   string
}

func (h *MultiExporter) Init(user string) {
	h.dbPool = make(map[int]*sql.DB)
	h.user = user
}

func (h *MultiExporter) connectDb(port int) error {
	var err error
	_, inited := h.dbPool[port]
	if inited {
		err = h.dbPool[port].Ping()
	}
	if !inited || err != nil {
		dsn := h.user + ":@(127.0.0.1:" + strconv.Itoa(port) + ")/"
		h.dbPool[port], err = sql.Open("mysql", dsn)
		if err = h.dbPool[port].Ping(); err != nil {
			return err
		}
		h.dbPool[port].Exec("set session wait_timeout=600")
	}
	return nil
}

func (h *MultiExporter) ScrapeMysql(c echo.Context) error {
	port, err := strconv.Atoi(c.Param("port"))
	if err != nil {
		c.Logger().Warn(err.Error())
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	if err = h.connectDb(port); err != nil {
		c.Logger().Warn(err.Error())
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	metricList := make(chan collector.Metric)
	go func(db *sql.DB, ch chan collector.Metric) {
		if err = collector.ScrapeProcesslist(db, ch); err != nil {
			c.Logger().Warn(err.Error())
		}
		if err = collector.ScrapeSlaveStatus(db, ch); err != nil {
			log.Println(err.Error())
			c.Logger().Warn(err.Error())
		}
		if err = collector.ScrapeInnodbTrx(db, ch); err != nil {
			c.Logger().Warn(err.Error())
		}
		if err = collector.ScrapeMysqlResource(db, ch); err != nil {
			c.Logger().Warn(err.Error())
		}
		close(ch)
	}(h.dbPool[port], metricList)

	rsp := genRsp(metricList)
	return c.JSON(http.StatusOK, rsp)
}

func (h *MultiExporter) ScrapeLinux(c echo.Context) error {
	metricList := make(chan collector.Metric)
	go func(ch chan collector.Metric) {
		if err := collector.ScrapeLinux(ch); err != nil {
			c.Logger().Warn(err)
		}
		close(ch)
	}(metricList)

	rsp := genRsp(metricList)
	return c.JSON(http.StatusOK, rsp)
}

func genRsp(metricList chan collector.Metric) map[string](map[string]interface{}) {
	rsp := make(map[string](map[string]interface{}))
	for metric := range metricList {
		if _, ok := rsp[metric.Namespace]; !ok {
			rsp[metric.Namespace] = make(map[string]interface{})
		}
		rsp[metric.Namespace][metric.Key] = metric.Value
	}
	return rsp
}
