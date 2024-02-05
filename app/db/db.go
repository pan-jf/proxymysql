package db

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

var (
	db *gorm.DB
)

func InitAdminDb() {
	dsn := "root:123456@tcp(127.0.0.1:3306)/test?loc=Local&charset=utf8mb4&parseTime=true"

	var err error
	db, err = gorm.Open(mysql.Open(dsn))
	if err != nil {
		panic(err)
	}

	sqlDb, _ := db.DB()
	sqlDb.SetConnMaxIdleTime(time.Hour)
	sqlDb.SetMaxIdleConns(4)

	err = sqlDb.Ping()
	if err != nil {
		panic(err)
	}

	initTable()

	//db = db.Debug()
}

func GetDB() *gorm.DB {
	return db
}

var (
	logTable = `CREATE TABLE if not exists sql_query_log (
    id integer PRIMARY KEY auto_increment,
    admin_id integer not null,
    admin_name text not null,
    admin_real_name text not null,
    query_game_id integer,   
    header_game_id integer,
    ip text,
    request_path text,
    request_info text,
    unix_milli integer,        
    query text,        
    create_time datetime default current_timestamp not null
)
`
)

func initTable() {
	if err := db.Exec(logTable).Error; err != nil {
		panic(err)
	}
}
