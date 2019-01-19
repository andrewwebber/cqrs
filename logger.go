package cqrs

type Logger interface {
	Debugf(v ...interface{})
	Println(v ...interface{})
	Printf(format string, v ...interface{})
}

type noopLogger struct{}

func (n noopLogger) Debugf(_ ...interface{}) {
}
func (n noopLogger) Println(_ ...interface{}) {
}

func (n noopLogger) Printf(format string, v ...interface{}) {
}

func defaultLogger() func() Logger {
	logger := &noopLogger{}
	return func() Logger {
		return logger
	}
}

var PackageLogger func() Logger = defaultLogger()
