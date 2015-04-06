package nsqlog

import (
	"github.com/astaxie/beego/logs"
)

//override Logger,LogLevel in go-nsq
type NsqLogger struct {
	*logs.BeeLogger
}

func NewNsqLogger() *NsqLogger {
	nsqLogger := new(NsqLogger)
	nsqLogger.BeeLogger = logs.NewLogger(1024)

	return nsqLogger
}

//use debug to write message
//override calldepth by global config
func (this *NsqLogger) Output(calldepth int, s string) error {
	this.Debug("calldepth[%d] %s", calldepth, s)
	return nil
}
