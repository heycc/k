package main

import (
	"k/multi_mysqld_exporter"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"os"
)

func main() {
	user := os.Getenv("user")
	port := os.Getenv("port")
	if user == "" {
		user = "root"
	}
	if port == "" {
		port = "8606"
	}

	log, err := os.OpenFile("exporter.log", os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0644)
	if err != nil {
		log = os.Stdout
	}

	e := echo.New()
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: "${time_rfc3339}, ${remote_ip}, ${method}, ${uri}, ${status}, ${latency_human}\n",
		Output: log,
	}))
	e.Use(middleware.Recover())

	h := &multiExporter.MultiExporter{}
	h.Init(user)

	e.GET("/mysql/:port", h.ScrapeMysql)
	e.GET("/linux", h.ScrapeLinux)
	e.Logger.Fatal(e.Start(":" + port))
}
