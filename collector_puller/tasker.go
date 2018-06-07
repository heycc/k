package collector_puller

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type Tasker struct {
	db       *sql.DB
	MysqlDsn string
	Logger   *Logger
	TaskChan chan ExporterEndPoint
	quitChan chan bool
}

func (t *Tasker) Start() {
	tickChan := time.NewTicker(time.Second * 5).C
	for {
		select {
		case <-t.quitChan:
			t.Logger.Info("tasker ", "quiting")
			return
		case <-tickChan:
			t.addTask()
		}
	}
}

func (t *Tasker) Stop() {
	t.quitChan <- true
}

func (t *Tasker) addTask() {
	var err error
	t.db, err = sql.Open("mysql", t.MysqlDsn)
	defer t.db.Close()
	if err = t.db.Ping(); err != nil {
		t.Logger.Error("add task(mysql) error, ", err.Error())
		return
	}
	t.db.Exec("set session wait_timeout=600")

	query := `select host_ip from t_keymetric_host 
				where lastupdate<from_unixtime(unix_timestamp(now())-inter_seconds+1)`
	rows, err := t.db.Query(query)
	defer rows.Close()
	if err != nil {
		t.Logger.Error("select t_keymetric_host error, ", err.Error())
		return
	}
	for rows.Next() {
		var ip string
		if err := rows.Scan(&ip); err != nil {
			t.Logger.Error("add task(host) error, ", err.Error())
		} else {
			t.TaskChan <- ExporterEndPoint{ip, "linux"}
		}
	}

	query = `select host_ip, port from t_keymetric_mysql
				where lastupdate<from_unixtime(unix_timestamp(now())-inter_seconds+1)`
	rows, err = t.db.Query(query)
	defer rows.Close()
	if err != nil {
		t.Logger.Error("select t_keymetric_mysql error, ", err.Error())
		return
	}
	for rows.Next() {
		var ip string
		var port []uint8
		if err := rows.Scan(&ip, &port); err != nil {
			t.Logger.Error("add task(mysql) error, ", err.Error())
		} else {
			t.TaskChan <- ExporterEndPoint{ip, "mysql/" + string(port)}
		}
	}
	return
}
