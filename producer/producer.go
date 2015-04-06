package producer

import (
	"encoding/json"
	"github.com/astaxie/beego/logs"

	"github.com/bitly/go-nsq"
	"github.com/vincent3i/nsqueue/nsqlog"
)

// Producer inherets the nsq Producer object
type Producer struct {
	Logger   *nsqlog.NsqLogger
	LogLevel *nsq.LogLevel

	*nsq.Producer
}

// New - Creates a new Producer.
func New() *Producer {
	return new(Producer)
}

// Connect method initialize the connection to nsq
func (p *Producer) Connect(addr string) (err error) {
	return p.ConnectConfig(addr, nsq.NewConfig())
}

// ConnectConfig method initialize the connection to nsq with config.
func (p *Producer) ConnectConfig(addr string, config *nsq.Config) (err error) {
	p.Producer, err = nsq.NewProducer(addr, config)
	p.Producer.SetLogger(p.logger(), p.loglevel())
	return
}

// PublishJSONAsync - sends message to nsq  topic in json format asynchronously
func (p *Producer) PublishJSONAsync(topic string, v interface{}, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.PublishAsync(topic, body, doneChan, args...)
}

// PublishJSON - sends message to nsq  topic in json format
func (p *Producer) PublishJSON(topic string, v interface{}) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.Publish(topic, body)
}

func (p *Producer) logger() *nsqlog.NsqLogger {
	if p.Logger == nil {
		return nsqlog.NewNsqLogger()
	}
	return p.Logger
}

func (p *Producer) loglevel() nsq.LogLevel {
	if p.LogLevel == nil {
		return logs.LevelDebug
	}
	return *p.LogLevel
}
