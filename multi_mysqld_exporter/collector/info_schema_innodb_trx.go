package collector

import (
	"database/sql"
)

const (
	innodbTrxQuery = `select count(*), COALESCE(max(trx_time),0), COALESCE(sum(trx_time),0)
		from (select unix_timestamp(now())-unix_timestamp(trx_started) trx_time
				from information_schema.innodb_trx
			)t`
)

func ScrapeInnodbTrx(db *sql.DB, ch chan Metric) error {
	var namespace = "innodbTrx"

	var (
		trx_cnt  uint64
		time_max uint64
		time_sum uint64
	)
	rows, err := db.Query(innodbTrxQuery)
	if err != nil {
		trx_cnt = 0
		time_max = 0
		time_sum = 0
	} else {
		defer rows.Close()
		for rows.Next() {
			if err := rows.Scan(&trx_cnt, &time_max, &time_sum); err != nil {
				return err
			}
		}
	}

	ch <- NewMetric(namespace, "count", trx_cnt)
	ch <- NewMetric(namespace, "sumTime", time_sum)
	ch <- NewMetric(namespace, "maxTime", time_max)
	return nil
}
