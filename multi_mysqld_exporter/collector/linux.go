package collector

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
)

func ScrapeLinux(ch chan Metric) error {
	namespace := "linux"
	if cpuNoIdle, err := getCpuNoIdle(); err == nil {
		ch <- NewMetric(namespace, "cpuNoIdle", cpuNoIdle)
	} else {
		return err
	}
	if upJiffies, err := getCpuJiffies("cpu0"); err == nil {
		ch <- NewMetric(namespace, "upJiffies", upJiffies)
	} else {
		return err
	}
	if load, err := getLoad(); err == nil {
		ch <- NewMetric(namespace, "loadavg", load)
	} else {
		return err
	}
	if uptime, err := getUptime(); err == nil {
		ch <- NewMetric(namespace, "uptime", uptime)
	} else {
		return err
	}
	if err := getDiskUtil(ch); err != nil {
		return err
	}
	return nil
}

func getUptime() (uint64, error) {
	data, err := ioutil.ReadFile("/proc/uptime")
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(string(data))
	if len(parts) < 2 {
		return 0, fmt.Errorf("unexpected content in /proc/uptime")
	}
	return strconv.ParseUint(strings.Split(parts[0], ".")[0], 10, 64)
}

func getLoad() (uint64, error) {
	data, err := ioutil.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(string(data))
	if len(parts) < 3 {
		return 0, fmt.Errorf("unexpected content in /proc/loadavg")
	}
	return strconv.ParseUint(strings.Split(parts[0], ".")[0], 10, 64)
}

func getCpuJiffies(cpuIdx string) (cpuJiffies uint64, err error) {
	data, err := ioutil.ReadFile("/proc/stat")
	if err != nil {
		return 0, err
	}
	var cpuLine []string
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.Fields(string(line))
		if parts[0] == cpuIdx {
			cpuLine = parts
			break
		}
	}
	for idx, val := range cpuLine {
		if idx == 0 {
			continue
		}
		if valNum, err := strconv.ParseUint(string(val), 10, 64); err != nil {
			return 0, err
		} else {
			cpuJiffies = cpuJiffies + valNum
		}
	}
	return cpuJiffies, nil
}

// cpu accumulated jeffies
func getCpuNoIdle() (cpuNoIdle uint64, err error) {
	data, err := ioutil.ReadFile("/proc/stat")
	if err != nil {
		return 0, err
	}
	cpuLine := strings.Fields(string(strings.Split(string(data), "\n")[0]))
	for idx, val := range cpuLine {
		if idx == 0 || idx == 4 {
			continue
		}
		if valNum, err := strconv.ParseUint(string(val), 10, 64); err != nil {
			return 0, err
		} else {
			cpuNoIdle = cpuNoIdle + valNum
		}
	}
	return cpuNoIdle, nil
}

func getDiskUtil(ch chan Metric) error {
	namespace := "ioutil"
	pattern := `^(sd[ab]|nvme[0-3]n1|hioa|fioa|cciss/c0d[01])$`

	data, err := ioutil.ReadFile("/proc/diskstats")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		parts := strings.Fields(string(line))
		if ok, _ := regexp.MatchString(pattern, parts[2]); ok {
			timeSpentDoingIO, err := strconv.ParseUint(string(parts[len(parts)-2]), 10, 64)
			if err != nil {
				return err
			}
			ch <- NewMetric(namespace, parts[2], uint64(timeSpentDoingIO))
		}
	}
	return nil
}
