package main

import (
	"flag"
	"log"
	"net"
	"os"
	"proxymysql/app/conf"
	"proxymysql/app/mysqlserver"
	"proxymysql/app/zlog"
	"time"
)

func main() {
	flag.StringVar(&conf.App.RemoteDb, "remote_db", "", "")
	flag.StringVar(&conf.App.ListenPort, "listen_port", ":5306", "")
	flag.StringVar(&conf.App.FilePath, "file_path", "", "")
	flag.StringVar(&conf.App.LogLevel, "log_level", zlog.InfoLevel, "日志级别 debug info error")
	flag.Parse()

	cfg := zlog.DefaultConfig
	cfg.Level = conf.App.LogLevel
	zlog.Init("app", cfg)

	if conf.App.RemoteDb == "" {
		zlog.Fatal("remote db addr not set")
	}

	zlog.Infof("remote db: %s", conf.App.RemoteDb)

	listen, err := net.Listen("tcp", conf.App.ListenPort)
	if err != nil {
		log.Fatal(err)
	}

	zlog.Infof("db server listen on: %s", conf.App.ListenPort)

	dirName := time.Now().Format("2006-01-02-15-04-05")

	dirPath := ""
	if conf.App.FilePath != "" {
		dirPath = conf.App.FilePath + string(os.PathSeparator) + dirName
	} else {
		currentPath, _ := os.Getwd()
		dirPath = currentPath + string(os.PathSeparator) + dirName
	}

	err = os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	zlog.Infof("create log path success: %s", dirPath)

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go func(conn2 net.Conn, dirPath string) {
			defer conn2.Close()

			err := mysqlserver.NewProxyConn(conn2, dirPath).Handle()
			if err != nil {
				zlog.Errorf("proxy conn handle err: %s", err)
			}

		}(conn, dirPath)
	}

}
