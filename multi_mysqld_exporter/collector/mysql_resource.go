package collector

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func ScrapeMysqlResource(db *sql.DB, ch chan Metric) error {
	namespace := "mysqlRes"
	upJiffies, err := getCpuJiffies("cpu0")
	if err != nil {
		return err
	}

	pid, err := getPid(db)
	if err != nil {
		return err
	}
	// get cpu and mem info for $pid mysqld
	statFile := "/proc/" + strings.TrimSpace(string(pid)) + "/stat"
	data, err := ioutil.ReadFile(statFile)
	if err != nil {
		return err
	}
	parts := strings.Fields(string(data))
	var totalTime int64
	for _, idx := range []int{13, 14, 15, 16} {
		val, err := strconv.ParseInt(parts[idx], 10, 64)
		if err != nil {
			return err
		}
		totalTime = totalTime + val
	}
	totalMem, err := strconv.ParseInt(parts[23], 10, 64)
	if err != nil {
		return err
	}
	totalMem = totalMem * int64(os.Getpagesize())

	ch <- NewMetric(namespace, "jiffies", totalTime)
	ch <- NewMetric(namespace, "mem", totalMem)
	ch <- NewMetric(namespace, "upJiffies", upJiffies)
	return nil
}

func getPid(db *sql.DB) (string, error) {
	sql := "select @@global.pid_file"
	pidRows, err := db.Query(sql)
	if err != nil {
		return "", err
	}
	defer pidRows.Close()

	// get pid of mysqld
	var pidFile string
	if !pidRows.Next() {
		return "", fmt.Errorf("get pidfile from db failed")
	}
	pidRows.Scan(&pidFile)
	pid, err := ioutil.ReadFile(pidFile)
	if err != nil {
		return "", err
	}
	return string(pid), nil
}
