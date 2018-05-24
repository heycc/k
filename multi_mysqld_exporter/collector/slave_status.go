package collector

import (
	"database/sql"
	"strings"
)

const (
	slaveStatusQuery  = "SHOW SLAVE STATUS"
	gtidExecutedQuery = `show global variables where Variable_name in ('gtid_mode','gtid_executed')`
)

var (
	slaveStatusCol = []string{
		"Master_UUID",
		"Slave_IO_Running",
		"Slave_SQL_Running",
		"Seconds_Behind_Master",
		"Last_Errno",
		"Last_Error",
	}
)

func columnIndex(slaveCols []string, colName string) int {
	for idx := range slaveCols {
		if strings.ToLower(slaveCols[idx]) == strings.ToLower(colName) {
			return idx
		}
	}
	return -1
}

func columnValue(scanArgs []interface{}, slaveCols []string, colName string) string {
	var columnIndex = columnIndex(slaveCols, colName)
	if columnIndex == -1 {
		return ""
	}
	return string(*scanArgs[columnIndex].(*sql.RawBytes))
}

func scrapeGtid(db *sql.DB, ch chan Metric) error {
	var namespace = "slaveStatus"

	gtidExecutedRows, err := db.Query(gtidExecutedQuery)
	if err != nil {
		return err
	}
	defer gtidExecutedRows.Close()

	var (
		var_name  string
		var_value string
	)
	var x = 0
	for gtidExecutedRows.Next() {
		gtidExecutedRows.Scan(&var_name, &var_value)
		ch <- NewMetric(namespace, var_name, var_value)
		x += 1
	}
	if x == 0 {
		ch <- NewMetric(namespace, "gtid_mode", "off")
		ch <- NewMetric(namespace, "gtid_executed", "")
	}
	return nil
}
func ScrapeSlaveStatus(db *sql.DB, ch chan Metric) error {
	var namespace = "slaveStatus"

	if err := scrapeGtid(db, ch); err != nil {
		return err
	}

	slaveStatusRows, err := db.Query(slaveStatusQuery)
	if err != nil {
		return err
	}
	defer slaveStatusRows.Close()

	slaveCols, err := slaveStatusRows.Columns()
	if err != nil {
		return err
	}

	var x = 0
	for slaveStatusRows.Next() {
		scanArgs := make([]interface{}, len(slaveCols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err := slaveStatusRows.Scan(scanArgs...); err != nil {
			return err
		}

		for _, col := range slaveStatusCol {
			colVal := columnValue(scanArgs, slaveCols, col)
			ch <- NewMetric(namespace, col, colVal)
		}
		x += 1
	}
	if x == 0 {
		for _, col := range slaveStatusCol {
			ch <- NewMetric(namespace, col, "")
		}
	}
	return nil
}
