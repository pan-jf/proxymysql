package conf

var App = &Config{}

type Config struct {
	RemoteDb   string
	ListenPort string
	LogLevel   string
	FilePath   string
}
