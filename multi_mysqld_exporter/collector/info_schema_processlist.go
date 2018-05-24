package collector

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

const infoSchemaProcesslistQuery = `
		SELECT COALESCE(command,''),COALESCE(state,''),count(*),sum(time)
		  FROM information_schema.processlist
		  WHERE ID != connection_id()
		    AND TIME >= %d
		  GROUP BY command,state
		  ORDER BY null
		`

var (
	// see https://dev.mysql.com/doc/refman/5.7/en/general-thread-states.html
	threadStateCounterMap = map[string]uint32{
		"checking":           uint32(0),
		"preparing":          uint32(0),
		"optimizing":         uint32(0),
		"statistics":         uint32(0),
		"cleaning up":        uint32(0),
		"end":                uint32(0),
		"opening tables":     uint32(0),
		"closing tables":     uint32(0),
		"flushing tables":    uint32(0),
		"altering table":     uint32(0),
		"creating table":     uint32(0),
		"creating tmp table": uint32(0),
		"deleting":           uint32(0),
		"executing":          uint32(0),
		"freeing items":      uint32(0),
		"updating":           uint32(0),
		"rolling back":       uint32(0),
		"sending data":       uint32(0),
		"sorting":            uint32(0),
		"idle":               uint32(0),
		"killed":             uint32(0),
		"user sleep":         uint32(0),
		"waiting for lock":   uint32(0),
		"waiting for tables": uint32(0),
		"logging slow query": uint32(0),
		"reading from net":   uint32(0),
		"writing to net":     uint32(0),
		"replication master": uint32(0),
		"other":              uint32(0),
	}
	threadStateMapping = map[string]string{
		"sorting for group":                        "sorting",
		"sorting for order":                        "sorting",
		"sorting index":                            "sorting",
		"sorting result":                           "sorting",
		"creating sort index":                      "sorting",
		"removing duplicates":                      "sorting",
		"converting heap to myisam":                "creating tmp table",
		"copying to tmp table":                     "creating tmp table",
		"copying to group table":                   "creating tmp table",
		"copy to tmp table":                        "creating tmp table",
		"removing tmp table":                       "creating tmp table",
		"after create":                             "creating table",
		"checking permissions":                     "checking",
		"checking table":                           "checking",
		"execution of init_command":                "executing",
		"repair by sorting":                        "altering table",
		"repair done":                              "altering table",
		"repair with keycache":                     "altering table",
		"creating index":                           "altering table",
		"committing alter table to storage engine": "altering table",
		"discard or import tablespace":             "altering table",
		"rename":                                   "altering table",
		"setup":                                    "altering table",
		"renaming result table":                    "altering table",
		"preparing for alter table":                "altering table",
		"manage keys":                              "altering table",
		"analyzing":                                "altering table",
		"repair":                                   "altering table",
		"update":                                   "updating",
		"updating main table":                      "updating",
		"updating reference tables":                "updating",
		"searching rows for update":                "updating",
		"deleting from main table":                 "deleting",
		"deleting from reference tables":           "deleting",
		"system lock":                              "waiting for lock",
		"user lock":                                "waiting for lock",
		"table lock":                               "waiting for lock",
		"waiting on cond":                          "waiting for lock",
		"waiting for table flush":                  "waiting for tables",
		"reopen tables":                            "opening tables",
		"query end":                                "end",
	}
	processlistMinTime = 0
)

func deriveThreadState(command string, state string) string {
	var normCmd = strings.Replace(strings.ToLower(command), "_", " ", -1)
	var normState = strings.Replace(strings.ToLower(state), "_", " ", -1)
	// check if it's already a valid state
	_, knownState := threadStateCounterMap[normState]
	if knownState {
		return normState
	}
	// check if plain mapping applies
	mappedState, canMap := threadStateMapping[normState]
	if canMap {
		return mappedState
	}
	// check special waiting for XYZ lock
	if strings.Contains(normState, "waiting for") && strings.Contains(normState, "lock") {
		return "waiting for lock"
	}
	if normCmd == "sleep" && normState == "" {
		return "idle"
	}
	if normCmd == "query" {
		return "executing"
	}
	if normCmd == "binlog dump" {
		return "replication master"
	}
	if normCmd == "connect" {
		return "replication slave"
	}
	return "other"
}

// ScrapeProcesslist collects from `information_schema.processlist`.
func ScrapeProcesslist(db *sql.DB, ch chan Metric) error {
	processQuery := fmt.Sprintf(
		infoSchemaProcesslistQuery,
		processlistMinTime,
	)
	processlistRows, err := db.Query(processQuery)
	if err != nil {
		return err
	}
	defer processlistRows.Close()

	var (
		command string
		state   string
		count   uint32
		time    uint32
	)
	stateCounts := make(map[string]uint32, len(threadStateCounterMap))
	stateTime := make(map[string]uint32, len(threadStateCounterMap))
	for k, v := range threadStateCounterMap {
		stateCounts[k] = v
		stateTime[k] = v
	}

	for processlistRows.Next() {
		err = processlistRows.Scan(&command, &state, &count, &time)
		if err != nil {
			return err
		}
		realState := deriveThreadState(command, state)
		stateCounts[realState] += count
		stateTime[realState] += time
	}

	topStateCounts := sortSlice(stateCounts)

	for idx, kv := range topStateCounts {
		state := kv.Key
		count := kv.Value
		// This means none connections
		if idx == 0 && count == 0 {
			ch <- NewMetric("plistCount", "idle", 1)
			ch <- NewMetric("plistTime", "idle", 0)
			break
		}
		// Return only active state
		if count == 0 {
			break
		}
		ch <- NewMetric("plistCount", state, count)
		ch <- NewMetric("plistTime", state, stateTime[state])
	}
	return nil
}

type kv struct {
	Key   string
	Value uint32
}

func sortSlice(m map[string]uint32) []kv {
	var ss []kv
	for k, v := range m {
		ss = append(ss, kv{k, v})
	}
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})
	return ss
}
