package zlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	DebugLevel = "DEBUG"
	InfoLevel  = "INFO"
	WarnLevel  = "WARN"
	ErrorLevel = "ERROR"
	FatalLevel = "FATAL"
)

var (
	levelMap = map[string]zapcore.Level{
		"DEBUG": zapcore.DebugLevel,
		"INFO":  zapcore.InfoLevel,
		"WARN":  zapcore.WarnLevel,
		"ERROR": zapcore.ErrorLevel,
		"FATAL": zapcore.FatalLevel,
	}
	// DefaultConfig 默认配置
	DefaultConfig = &Config{
		Level:         DebugLevel,
		NeedLogFile:   false,
		ConsoleWriter: os.Stdout,
	}

	DefaultLogFile = &LogFile{
		LogFilePath: "logs",
		MaxSize:     200,
		MaxAge:      0,
		MaxBackups:  0,
	}
	globalLog = NewLogger("app", DefaultConfig)
)

type (
	ZapLog struct {
		sugarLog *zap.SugaredLogger
	}

	Config struct {
		Level         string
		NeedLogFile   bool
		ConsoleWriter io.Writer
		ZapOpt        []zap.Option
		LogFile       *LogFile
	}

	LogFile struct {
		LogFilePath string
		MaxSize     int
		MaxAge      int
		MaxBackups  int
	}
)

// Init 覆盖默认日志
func Init(serverName string, config *Config) {
	globalLog = NewLogger(serverName, config)
}

func NewLogger(serverName string, config *Config) *ZapLog {
	if config == nil {
		config = DefaultConfig
	}
	var level zapcore.Level
	if v, ok := levelMap[strings.ToUpper(config.Level)]; ok {
		level = v
	} else {
		level = zapcore.DebugLevel
	}

	cores := make([]zapcore.Core, 0)
	// 使用控制台输出
	if config.ConsoleWriter != nil {
		cfg := zap.NewProductionEncoderConfig()
		cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		cfg.ConsoleSeparator = " | "
		// 指定日志时间格式
		cfg.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000")
		cfg.EncodeCaller = zapcore.ShortCallerEncoder
		encoder := zapcore.NewConsoleEncoder(cfg)
		core := zapcore.NewCore(encoder, zapcore.AddSync(config.ConsoleWriter), level)
		cores = append(cores, core)
	}

	if config.NeedLogFile {
		cfg := zap.NewProductionEncoderConfig()
		cfg.EncodeLevel = zapcore.CapitalLevelEncoder
		cfg.ConsoleSeparator = " | "
		// 指定日志时间格式
		cfg.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000")
		cfg.EncodeCaller = zapcore.ShortCallerEncoder
		encoder := zapcore.NewConsoleEncoder(cfg)
		core := zapcore.NewCore(encoder, zapcore.AddSync(getRollingFileWriter(serverName, config)), level)
		cores = append(cores, core)
	}

	opts := make([]zap.Option, 0)
	if config.ZapOpt != nil {
		opts = config.ZapOpt
	} else {
		opts = append(opts, zap.AddCaller(), zap.AddCallerSkip(2))
	}

	zl := zap.New(zapcore.NewTee(cores...), opts...)

	return &ZapLog{
		sugarLog: zl.Sugar(),
	}
}

func getRollingFileWriter(serverName string, config *Config) *lumberjack.Logger {
	if config.LogFile == nil {
		config.LogFile = DefaultLogFile
	}

	return &lumberjack.Logger{
		Filename:   filepath.Join(config.LogFile.LogFilePath, serverName+".log"),
		MaxSize:    config.LogFile.MaxSize,
		MaxAge:     config.LogFile.MaxAge,
		MaxBackups: config.LogFile.MaxBackups,
		LocalTime:  true,
		Compress:   false,
	}
}

func (z *ZapLog) Debug(args ...interface{}) {
	z.sugarLog.Debug(args...)
}

func (z *ZapLog) Info(args ...interface{}) {
	z.sugarLog.Info(args...)
}

func (z *ZapLog) Warn(args ...interface{}) {
	z.sugarLog.Warn(args...)
}

func (z *ZapLog) Error(args ...interface{}) {
	z.sugarLog.Error(args...)
}

func (z *ZapLog) Fatal(args ...interface{}) {
	z.sugarLog.Fatal(args...)
}

func (z *ZapLog) Debugf(format string, args ...interface{}) {
	z.sugarLog.Debugf(format, args...)
}

func (z *ZapLog) Infof(format string, args ...interface{}) {
	z.sugarLog.Infof(format, args...)
}

func (z *ZapLog) Warnf(format string, args ...interface{}) {
	z.sugarLog.Warnf(format, args...)
}

func (z *ZapLog) Errorf(format string, args ...interface{}) {
	z.sugarLog.Errorf(format, args...)
}

func (z *ZapLog) Fatalf(format string, args ...interface{}) {
	z.sugarLog.Fatalf(format, args...)
}

func (z *ZapLog) Println(args ...interface{}) {
	z.sugarLog.Info(args...)
}

func (z *ZapLog) Printf(format string, args ...interface{}) {
	z.sugarLog.Infof(format, args...)
}

func (z *ZapLog) Sync() {
	z.sugarLog.Sync()
}

func Debug(args ...interface{}) {
	globalLog.Debug(args...)
}

func Info(args ...interface{}) {
	globalLog.Info(args...)
}

func Warn(args ...interface{}) {
	globalLog.Warn(args...)
}

func Error(args ...interface{}) {
	globalLog.Error(args...)
}

func Fatal(args ...interface{}) {
	globalLog.Fatal(args...)
}

func Debugf(format string, args ...interface{}) {
	globalLog.Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	globalLog.Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	globalLog.Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	globalLog.Errorf(format, args...)
}

func Fatalf(format string, args ...interface{}) {
	globalLog.Fatalf(format, args...)
}

func Println(args ...interface{}) {
	globalLog.Info(args...)
}

func Printf(format string, args ...interface{}) {
	globalLog.Infof(format, args...)
}

func GetLogger() *ZapLog {
	return globalLog
}

func Flush() {
	globalLog.Sync()
}
