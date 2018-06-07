package collector_puller

import (
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type PullerWorker struct {
	MysqlDsn     string
	Logger       *Logger
	ExporterPort int
	TaskChan     chan ExporterEndPoint
	quitChan     chan bool
	Client       *http.Client
	WokerId      int
	db           *sql.DB
}

func (w *PullerWorker) Start() {
	w.quitChan = make(chan bool, 1)

	var err error
	w.db, err = sql.Open("mysql", w.MysqlDsn)
	defer w.db.Close()
	if err = w.db.Ping(); err != nil {
		w.Logger.Error("connect to config db: "+w.MysqlDsn+" failed, ", err.Error())
		return
	}
	w.db.Exec("set session wait_timeout=600")
	w.db.Exec("set autocommit=1")

	for {
		select {
		case endPoint := <-w.TaskChan:
			url := "http://" + endPoint.Host + ":" + strconv.Itoa(w.ExporterPort) + "/" + endPoint.Uri
			rsp, err := w.Client.Get(url)
			if err != nil {
				w.Logger.Error(err.Error())
				w.updateState(endPoint.Host, endPoint.Uri, nil, err)
				continue
			}

			body, err := ioutil.ReadAll(rsp.Body)
			rsp.Body.Close()
			if err != nil {
				w.Logger.Error("read rsp body error, ", url, err.Error())
				w.updateState(endPoint.Host, endPoint.Uri, nil, errors.New(err.Error()))
			} else if code := rsp.StatusCode; code != 200 {
				w.Logger.Error("http rsp not OK.", endPoint.Host, endPoint.Uri, rsp.Status)
				w.updateState(endPoint.Host, endPoint.Uri, nil, errors.New(string(body)))
			} else {
				w.Logger.Debug("get rsp code OK, ", endPoint.Host, endPoint.Uri, code)
				w.updateState(endPoint.Host, endPoint.Uri, body, nil)
			}

		case <-w.quitChan:
			w.Logger.Info("woker ", w.WokerId, "quiting")
			return
		}
	}
}

func (w *PullerWorker) Stop() {
	w.quitChan <- true
}

func (w *PullerWorker) updateState(host string, uri string, body []byte, pullError error) {
	if uri == "linux" {
		err := w.updateLinux(host, uri, body, pullError)
		if err != nil {
			w.Logger.Error("updateLinux error, ", host, uri, err.Error())
		}
	} else if uri[0:5] == "mysql" {
		err := w.updateMysql(host, uri, body, pullError)
		if err != nil {
			w.Logger.Error("updateMysql error, ", host, uri, err.Error())
		}
	} else {
		w.Logger.Warn("unknown uri prefix, ", host, uri)
	}
}

func (w *PullerWorker) updateMysqlError(host string, uri string, e error) error {
	port := strings.Split(uri, "/")[1]
	if e != nil {
		w.Logger.Debug("updateMysqlError", host, uri, e.Error())
		query := `update t_keymetric_mysql 
					set error=?, lastupdate=now()
					where host_ip=? and port=?`
		_, err := w.db.Exec(query, e.Error(), host, port)
		return err
	}
	return nil
}

func (w *PullerWorker) updateMysql(host string, uri string, body []byte, pullError error) error {
	if pullError != nil {
		w.Logger.Debug("updateMysql, ", host, uri, string(body), pullError.Error())
	} else {
		w.Logger.Debug("updateMysql, ", host, uri, string(body))
	}
	port := strings.Split(uri, "/")[1]

	if pullError != nil {
		return w.updateMysqlError(host, uri, pullError)
	}

	var body_map map[string]interface{}
	if err := json.Unmarshal(body, &body_map); err != nil {
		return err
	}
	innodb_Map, ok := body_map["innodbTrx"].(map[string]interface{})
	if !ok {
		return w.updateMysqlError(host, uri, errors.New("innodbTrx assertion error"))
	}
	mysqlRes_map, ok := body_map["mysqlRes"].(map[string]interface{})
	if !ok {
		return w.updateMysqlError(host, uri, errors.New("mysqlRes assertion error"))
	}
	plistCount_map, ok := body_map["plistCount"].(map[string]interface{})
	if !ok {
		return w.updateMysqlError(host, uri, errors.New("plistCount assertion error"))
	}
	plistTime_map, ok := body_map["plistTime"].(map[string]interface{})
	if !ok {
		return w.updateMysqlError(host, uri, errors.New("plistTime assertion error"))
	}
	slaveStatus_map, ok := body_map["slaveStatus"].(map[string]interface{})
	if !ok {
		return w.updateMysqlError(host, uri, errors.New("slaveStatus assertion error"))
	}

	var o_raw_data []byte
	var n_cpu_rate float64

	query := `select raw_data from t_keymetric_mysql where host_ip=? and port=?`
	err := w.db.QueryRow(query, host, port).Scan(&o_raw_data)
	switch {
	case err == sql.ErrNoRows || err != nil:
		n_cpu_rate = 0
	default:
		if string(o_raw_data) == "" {
			n_cpu_rate = 0
			break
		}
		o_map := make(map[string]interface{})
		err = json.Unmarshal(o_raw_data, &o_map)
		if err != nil {
			return err
		}
		o_mysqlRes_map := o_map["mysqlRes"].(map[string]interface{})
		n_cpu_rate = 100 * (mysqlRes_map["jiffies"].(float64) - o_mysqlRes_map["jiffies"].(float64)) /
			(mysqlRes_map["upJiffies"].(float64) - o_mysqlRes_map["upJiffies"].(float64))
	}

	plist_state := "idle"
	plist_count := float64(0)
	for k := range plistCount_map {
		if k != "idle" && plist_state == "idle" {
			plist_count = plistCount_map[k].(float64)
			plist_state = k
		} else if k != "idle" && plist_state != "idle" && plistCount_map[k].(float64) >= plist_count {
			plist_count = plistCount_map[k].(float64)
			plist_state = k
		} else if k == "idle" && plist_state == "idle" {
			plist_count = plistCount_map[k].(float64)
			plist_state = k
		}
	}
	plist_sumtime := plistTime_map[plist_state].(float64)

	var slave_gtid_behind int64
	gtid_mode, ok := slaveStatus_map["gtid_mode"].(string)
	if ok && gtid_mode == "ON" {
		gtid_lag, err := w.getMysqlGtidBehind(host, port, slaveStatus_map["gtid_executed"].(string))
		if err != nil {
			return w.updateMysqlError(host, uri, err)
			slave_gtid_behind = -1
		} else {
			slave_gtid_behind = gtid_lag
		}
	} else if ok {
		slave_gtid_behind = 0
	} else {
		return w.updateMysqlError(host, uri, errors.New("slave status gtid_mode assertion error"))
	}

	query = `insert into t_keymetric_mysql_history(
    				ts, host_ip, port,
    				innodb_trx_cnt, innodb_trx_maxtime, innodb_trx_sumtime,
    				proc_mem, proc_cpu,
    				slave_last_errno, slave_last_error,
    				slave_io_running, slave_sql_running,
    				slave_seconds_behind_master, slave_gtid_behind,
    				plist_state, plist_count, plist_sumtime)
			select ts, host_ip, port,
    				innodb_trx_cnt, innodb_trx_maxtime, innodb_trx_sumtime,
    				proc_mem, proc_cpu,
    				slave_last_errno, slave_last_error,
    				slave_io_running, slave_sql_running,
    				slave_seconds_behind_master, slave_gtid_behind,
    				plist_state, plist_count, plist_sumtime
    		from t_keymetric_mysql where host_ip=? and port=?`
	_, err = w.db.Exec(query, host, port)
	if err != nil {
		return err
	}

	query = `insert into t_keymetric_mysql(
    				ts, host_ip, port,
    				innodb_trx_cnt, innodb_trx_maxtime, innodb_trx_sumtime,
    				proc_mem, proc_cpu,
    				slave_last_errno, slave_last_error,
    				slave_io_running, slave_sql_running,
    				slave_seconds_behind_master, slave_gtid_behind,
    				plist_state, plist_count, plist_sumtime,
    				raw_data
    			)
			values(now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			on duplicate key update 
				innodb_trx_cnt=values(innodb_trx_cnt),
				innodb_trx_maxtime=values(innodb_trx_maxtime),
				innodb_trx_sumtime=values(innodb_trx_sumtime),
				proc_cpu=values(proc_cpu),
				proc_mem=values(proc_mem),
				slave_last_errno=values(slave_last_errno),
				slave_last_error=values(slave_last_error),
				slave_seconds_behind_master=values(slave_seconds_behind_master),
				slave_io_running=values(slave_io_running),
				slave_sql_running=values(slave_sql_running),
				slave_gtid_behind=values(slave_gtid_behind),
				plist_state=values(plist_state),
				plist_count=values(plist_count),
				plist_sumtime=values(plist_sumtime),
				raw_data=values(raw_data), error=NULL, ts=now()
				`
	_, err = w.db.Exec(query,
		host, port,
		innodb_Map["count"].(float64), innodb_Map["maxTime"].(float64), innodb_Map["sumTime"].(float64),
		mysqlRes_map["mem"].(float64), n_cpu_rate,
		slaveStatus_map["Last_Errno"].(string), slaveStatus_map["Last_Error"].(string),
		slaveStatus_map["Slave_IO_Running"].(string), slaveStatus_map["Slave_SQL_Running"].(string),
		slaveStatus_map["Seconds_Behind_Master"].(string), slave_gtid_behind,
		plist_state, plist_count, plist_sumtime,
		body)
	if err != nil {
		return err
	}
	return nil
}

func (w *PullerWorker) getMysqlGtidBehind(host string, port string, slaveGtid string) (int64, error) {
	var (
		s_master_ip   string
		s_master_port []uint8
		m_raw_data    []byte
	)
	query := `select master_ip,master_port
			from t_app_inst 
			where slave_ip=? and slave_port=? 
			union 
			select master_ip,master_port 
			from t_app_inst i,t_slave_ro r 
			where r.instance_id=i.instance_id and r.slave_ip=? and r.slave_port=?`
	err := w.db.QueryRow(query, host, port, host, port).Scan(&s_master_ip, &s_master_port)
	switch {
	case err == sql.ErrNoRows:
		w.Logger.Debug("Not found master info, ", host, port)
		return 0, nil
	case err != nil:
		return 0, err
	default:
		query = `select raw_data from t_keymetric_mysql
				where host_ip = ? and port =? and error is null`
		err = w.db.QueryRow(query, s_master_ip, s_master_port).Scan(&m_raw_data)
		switch {
		case err == sql.ErrNoRows || err != nil || string(m_raw_data) == "":
			return 0, errors.New("Not found master gtid info")
		default:
			m_map := make(map[string]interface{})
			err = json.Unmarshal(m_raw_data, &m_map)
			if err != nil {
				return 0, err
			}
			slaveStatus_map, ok := m_map["slaveStatus"].(map[string]interface{})
			if !ok {
				return 0, errors.New("slaveStatus assertion error")
			}
			m_gtid := slaveStatus_map["gtid_executed"].(string)

			// Sometimes GTID_EXECUTED may be empty in M-S relationship
			if m_gtid == "" {
				return 0, nil
			}
			if len(m_gtid) <= 37 {
				return 0, errors.New("Unexpected gtid length, " + m_gtid)
			}
			m_gtid = strings.Replace(m_gtid, "\\n", "", -1)
			slaveGtid = strings.Replace(slaveGtid, "\\n", "", -1)

			var gtidTotalLag int64
			gtidTotalLag = 0

			m_gtid_map := make(map[string]int64)
			for _, gtidSeq := range strings.Split(m_gtid, ",") {
				if len(gtidSeq) <= 37 {
					return 0, errors.New("Unexpected master gtid length, " + gtidSeq)
				}
				gtidArr := strings.Split(gtidSeq, ":")
				idArr := strings.Split(gtidArr[len(gtidArr)-1], "-")
				m_gtid_map[gtidArr[0]], _ = strconv.ParseInt(idArr[len(idArr)-1], 10, 64)
			}
			for _, gtidSeq := range strings.Split(slaveGtid, ",") {
				if len(gtidSeq) <= 37 {
					return 0, errors.New("Unexpected slave gtid length, " + gtidSeq)
				}
				gtidArr := strings.Split(gtidSeq, ":")
				idArr := strings.Split(gtidArr[len(gtidArr)-1], "-")
				if m_max_id, ok := m_gtid_map[gtidArr[0]]; ok {
					s_max_id, _ := strconv.ParseInt(idArr[len(idArr)-1], 10, 64)
					if m_max_id > s_max_id {
						gtidTotalLag = gtidTotalLag + (m_max_id - s_max_id)
					}
				}
			}
			return gtidTotalLag, nil
		}
	}
}

func (w *PullerWorker) updateLinuxError(host string, uri string, e error) error {
	if e != nil {
		w.Logger.Debug("updateLinuxError, ", host, uri)
		query := `update t_keymetric_host
					set error=?, lastupdate=now()
					where host_ip=?`
		_, err := w.db.Exec(query, e.Error(), host)
		return err
	}
	return nil
}

func (w *PullerWorker) updateLinux(host string, uri string, body []byte, pullError error) error {
	if pullError != nil {
		w.Logger.Debug("updateMysql, ", host, uri, string(body), pullError.Error())
	} else {
		w.Logger.Debug("updateMysql, ", host, uri, string(body))
	}
	if pullError != nil {
		return w.updateLinuxError(host, uri, pullError)
	}

	var body_map map[string]interface{}
	err := json.Unmarshal(body, &body_map)
	if err != nil {
		return err
	}

	body_ioutil, ok := body_map["ioutil"].(map[string]interface{})
	if !ok {
		return w.updateLinuxError(host, uri, errors.New("ioutil assertion error"))
		return nil
	}
	body_linux, ok := body_map["linux"].(map[string]interface{})
	if !ok {
		return w.updateLinuxError(host, uri, errors.New("linux assertion error"))
		return nil
	}

	var o_raw_data []byte
	var n_cpu_rate float64
	var n_ioutil_max float64

	query := `select raw_data from t_keymetric_host where host_ip=?`
	err = w.db.QueryRow(query, host).Scan(&o_raw_data)
	switch {
	case err == sql.ErrNoRows || err != nil:
		n_cpu_rate = 0
		n_ioutil_max = 0
	default:
		if string(o_raw_data) == "" {
			n_cpu_rate = 0
			n_ioutil_max = 0
			break
		}
		o_map := make(map[string]interface{})
		err = json.Unmarshal(o_raw_data, &o_map)
		if err != nil {
			return err
		}
		o_linux := o_map["linux"].(map[string]interface{})
		o_ioutil := o_map["ioutil"].(map[string]interface{})
		// cpu_busy_time is in ms, uptime is in s
		uptime_diff := (body_linux["uptime"].(float64) - o_linux["uptime"].(float64)) * 1000
		n_cpu_rate = 100 *
			(body_linux["cpuNoIdle"].(float64) - o_linux["cpuNoIdle"].(float64)) /
			(body_linux["upJiffies"].(float64) - o_linux["upJiffies"].(float64))
		n_ioutil_max = 100 * w.maxDiskUtilDiff(body_ioutil, o_ioutil) / uptime_diff
	}

	query = `insert into t_keymetric_host_history(ts, host_ip,loadavg,cpu_rate,ioutil_max)
			select ts,host_ip,loadavg,cpu_rate,ioutil_max
			from t_keymetric_host where host_ip=?`
	_, err = w.db.Exec(query, host)
	if err != nil {
		return err
	}
	query = `insert into t_keymetric_host(ts, host_ip, loadavg, cpu_rate, ioutil_max, raw_data)
		values(now(),?,?,?,?,?)
		on duplicate key update 
			ts = now(),
			loadavg=values(loadavg),
			cpu_rate=values(cpu_rate),
			raw_data=values(raw_data),
			ioutil_max=values(ioutil_max),error=NULL`
	_, err = w.db.Exec(query, host,
		body_map["linux"].(map[string]interface{})["loadavg"].(float64),
		n_cpu_rate, n_ioutil_max, body)
	if err != nil {
		return err
	}
	return nil
}

func (w *PullerWorker) maxDiskUtilDiff(n, o map[string]interface{}) float64 {
	maxVal := float64(0)
	for k, v := range n {
		if maxVal < v.(float64)-o[k].(float64) {
			maxVal = v.(float64) - o[k].(float64)
		}
	}
	return maxVal
}
