package mysqlserver

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	jsoniter "github.com/json-iterator/go"
	"io"
	"os"
	"proxymysql/app/zlog"
	"regexp"
	"strings"
	"time"
)

var _ io.Writer = (*RecordQuery)(nil)

type RecordQuery struct {
	file       *os.File
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	stmtId     uint32
	stmtMap    map[uint32]string
}

func NewRecordQuery(clientIp string, dirPath string) *RecordQuery {
	clientPort := strings.Replace(clientIp, "127.0.0.1:", "", -1)
	fileName := dirPath + "/" + clientPort + ".log"
	zlog.Infof("create file:%s", fileName)

	file, err2 := os.Create(fileName)
	if err2 != nil {
		zlog.Fatalf(err2.Error())
	}

	r := &RecordQuery{}
	r.pipeReader, r.pipeWriter = io.Pipe()
	r.file = file
	r.stmtMap = make(map[uint32]string)
	go r.readQuery()
	return r
}

func (r *RecordQuery) Write(p []byte) (n int, err error) {
	r.pipeWriter.Write(p)

	return len(p), err
}

func (r *RecordQuery) Close() error {
	r.pipeWriter.Close()
	r.pipeReader.Close()

	return nil
}

func (r *RecordQuery) readQuery() {
	for {
		packet, err := ReadMysqlPacket(r.pipeReader)
		if err != nil {
			// errors.Is(err, io.ErrClosedPipe)
			zlog.Errorf("read pipe pack err: %s", err)
			return
		}

		if len(packet.Payload) < 2 {
			continue
		}

		switch packet.Payload[0] {
		case ComQuery:
			query := string(packet.Payload[1:])

			zlog.Debugf("query: %s\n", query)

			r.saveToDb(query)

		case ComPrepare:
			query := string(packet.Payload[1:])

			write := bufio.NewWriter(r.file)
			_, _ = write.WriteString(fmt.Sprintf("【%s】【PREPARE】 %s\n",
				time.Now().Format("2006-01-02 15:04:05.000"), query))
			_ = write.Flush()

			zlog.Infof("prepare %s\n", query)
			r.stmtId++
			r.stmtMap[r.stmtId] = query

			r.saveToDb(query)

		case ComStmtExecute:
			query := r.stmtMap[r.stmtId]

			_, args := r.parseStmtArgs(strings.Count(query, "?"), packet.Payload)

			//fmt.Printf("ComStmtExecute: %s %+v\n", query, args)

			fullSqlQuery, err := sqlbuilder.MySQL.Interpolate(query, args)
			if err != nil {
				zlog.Errorf("ComStmtExecute builder sql err: %s", err)
			} else {
				write := bufio.NewWriter(r.file)
				_, _ = write.WriteString(fmt.Sprintf("【%s】【FULLSQL】 %s\n",
					time.Now().Format("2006-01-02 15:04:05.000"), fullSqlQuery))
				_ = write.Flush()

				zlog.Infof("stmt: %s\n", fullSqlQuery)
			}

			r.saveToDb(fullSqlQuery)

		case ComStmtClose:
			delete(r.stmtMap, r.stmtId)
		}

	}

}

type BindArg struct {
	ArgType  uint8
	Unsigned uint8
	ArgValue interface{}
}

func (r *RecordQuery) parseStmtArgs(argNum int, data []byte) ([]*BindArg, []any) {
	if argNum == 0 {
		return nil, nil
	}

	if len(data) == 0 {
		return nil, nil
	}

	skipPos := 1 + 4 + 1 + 4

	buf := bytes.NewBuffer(data)

	//fmt.Printf("%+v\n", buf.Bytes())

	buf.Next(skipPos)

	nullBitMapLen := (argNum + 7) / 8
	//fmt.Println(nullBitMapLen)

	nullBitMap := buf.Next(nullBitMapLen)
	//fmt.Println("nullBitMap", nullBitMap)

	newParamsBindFlag := ReadByte(buf.Next(1))
	//fmt.Println("newParamsBindFlag", ReadByte(newParamsBindFlag))

	if newParamsBindFlag != 0x01 {
		return nil, nil
	}

	bindArgs := make([]*BindArg, argNum)
	args := make([]interface{}, argNum)

	for i := 0; i < argNum; i++ {
		filedType := ReadByte(buf.Next(1))
		//fmt.Printf("filedType: %+v\n", filedType)

		unsigned := ReadByte(buf.Next(1))
		//fmt.Printf("unsigned: %+v\n", unsigned)

		bindArgs[i] = &BindArg{
			ArgType:  filedType,
			Unsigned: unsigned,
			ArgValue: nil,
		}
	}

	//fmt.Printf("val: %+v\n", buf.Bytes())

	//fmt.Printf("%+v\n", nullBitMap)

	for i := 0; i < argNum; i++ {
		nullBytePos := i / 8
		nullBitPos := i % 8

		//fmt.Printf("nullBytePos: %08b\n", nullBitMap[nullBytePos])
		//fmt.Printf("nullBitPos:  %08b\n", 1<<nullBitPos)

		if (nullBitMap[nullBytePos] & (1 << nullBitPos)) > 0 {
			//buf.Next(1)
			bindArgs[i].ArgValue = nil
			args[i] = nil

			//fmt.Printf("%+v\n", bindArgs[i])

			continue
		}

		switch bindArgs[i].ArgType {

		case FieldTypeTiny, FieldTypeBit:
			val := ReadByte(buf.Next(1))

			bindArgs[i].ArgValue = val
			args[i] = val

		case FieldTypeInt24, FieldTypeLong:
			val := ReadUint32(buf.Next(4))

			bindArgs[i].ArgValue = val
			args[i] = val

		case FieldTypeLongLong:
			val := ReadUint64(buf.Next(8))

			bindArgs[i].ArgValue = val
			args[i] = val

		default:
			length, pos, ok := ReadLengthEncodedInt(buf.Bytes())
			if !ok {
				zlog.Errorf("read args err %+v", buf.Bytes())
				continue
			}

			buf.Next(pos)
			val := string(buf.Next(int(length)))

			bindArgs[i].ArgValue = val
			args[i] = val

			//fmt.Printf("str: %s\n", val)
		}

	}

	return bindArgs, args
}

type SqlComment struct {
	//AdminId       int64
	//AdminName     string
	//AdminRealName string
	//QueryGameId  int32
	//HeaderGameId int32
	//Ip           string
	//RequestPath string
	//RequestInfo string
	UnixMilli int64
	Query     string
	//CallInfo   string
	CreateTime string
}

var adminCommentReg = regexp.MustCompile(`/\*\s+TzAdmin-([\s\S]+)-TzAdmin\s+\*/`)

func (r *RecordQuery) parseSqlComment(query string) (sc *SqlComment) {
	sc = &SqlComment{}
	sc.Query = query
	if sc.UnixMilli == 0 {
		sc.UnixMilli = time.Now().UnixMilli()
	}

	sc.CreateTime = time.Now().Format("2006-01-02 15:04:05.000")

	if !strings.Contains(query, " TzAdmin-") {
		return
	}

	subMatch := adminCommentReg.FindStringSubmatch(query)

	if len(subMatch) >= 2 {
		err := jsoniter.Unmarshal([]byte(subMatch[1]), sc)
		if err != nil {
			zlog.Warnf("解析sql admin信息失败 %s [%s]", err, subMatch[1])
			return
		}

		_sql := strings.TrimSpace(adminCommentReg.ReplaceAllString(sc.Query, ""))
		sc.Query = _sql
		//zlog.Infof("%+v\n", sc)
	}

	return
}

func (r *RecordQuery) saveToDb(query string) {
	return
}
