package collector_puller

import (
	"log"
	"os"
)

type Logger struct {
	lg          *log.Logger
	logfile     string
	defaultFlag int
	logLevelMap map[string]int
	logLevel    string
}

func (l *Logger) Init(f string, level string) {
	l.logLevelMap = map[string]int{
		"DEBUG": 1,
		"INFO":  2,
		"WARN":  3,
		"ERROR": 4,
		"FATAL": 5,
	}
	l.logfile = f
	l.logLevel = level
	logf, err := os.OpenFile(l.logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Panic("open log file failed.", err)
	}
	l.defaultFlag = log.Ldate | log.Ltime | log.Lshortfile
	l.lg = log.New(logf, "INFO", l.defaultFlag)
}

func (l *Logger) Debug(v ...interface{}) {
	if l.logLevelMap[l.logLevel] > l.logLevelMap["DEBUG"] {
		return
	}
	l.lg.SetPrefix("DEBUG ")
	l.lg.SetFlags(log.Ldate | log.Ltime)
	l.lg.Print(v)
	l.lg.SetFlags(l.defaultFlag)
}

func (l *Logger) Info(v ...interface{}) {
	if l.logLevelMap[l.logLevel] > l.logLevelMap["INFO"] {
		return
	}
	l.lg.SetPrefix("INFO ")
	l.lg.SetFlags(log.Ldate | log.Ltime)
	l.lg.Print(v)
	l.lg.SetFlags(l.defaultFlag)
}

func (l *Logger) Warn(v ...interface{}) {
	if l.logLevelMap[l.logLevel] > l.logLevelMap["WARN"] {
		return
	}
	l.lg.SetPrefix("WARN ")
	l.lg.Print(v)
}

func (l *Logger) Error(v ...interface{}) {
	if l.logLevelMap[l.logLevel] > l.logLevelMap["ERROR"] {
		return
	}
	l.lg.SetPrefix("ERROR ")
	l.lg.Print(v)
}

func (l *Logger) Fatal(v ...interface{}) {
	if l.logLevelMap[l.logLevel] > l.logLevelMap["FATAL"] {
		return
	}
	l.lg.SetPrefix("FATAL ")
	l.lg.Fatal(v)
}
