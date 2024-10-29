package logger

type Logger interface {
	Printf(format string, v ...interface{}) (n int, err error)
	Errorf(format string, v ...interface{}) (n int, err error)
}
