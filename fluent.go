package logrus_fluent

import (
	"github.com/bluearchive/logrus"
	"github.com/bluearchive/fluent-logger-golang/fluent"
	"sync"
	"fmt"
)

const (
	TagName      = "fluent"
	TagField     = "tag"
	MessageField = "message"
)

var defaultLevels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
}

type FluentHook struct {
	config fluent.Config
	host   string
	port   int
	levels []logrus.Level
	tag    *string
	mu     sync.Mutex
	logger *fluent.Fluent
}

func NewHook(config fluent.Config) *FluentHook {
	return &FluentHook{
		config: config,
		levels: defaultLevels,
		tag:    nil,
	}
}

func getTagAndDel(entry *logrus.Entry, data logrus.Fields) string {
	var v interface{}
	var ok bool
	if v, ok = data[TagField]; !ok {
		return entry.Message
	}

	var val string
	if val, ok = v.(string); !ok {
		return entry.Message
	}
	delete(data, TagField)
	return val
}

func setLevelString(entry *logrus.Entry, data logrus.Fields) {
	data["level"] = entry.Level.String()
}

func setMessage(entry *logrus.Entry, data logrus.Fields) {
	if _, ok := data[MessageField]; !ok {
		data[MessageField] = entry.Message
	}
}

func (this *FluentHook) Name() string {
	return "Fluent"
}

func (hook *FluentHook) Fire(entry *logrus.Entry) error {
	// Create a map for passing to FluentD
	data := make(logrus.Fields)
	for k, v := range entry.Data {
		data[k] = fmt.Sprintf("%v", v)
	}

	setLevelString(entry, data)
	var tag string
	tag = *hook.tag
	if tag != entry.Message {
		setMessage(entry, data)
	}

	fluentData := ConvertToValue(data, TagName)

	// serialize control of the connection creation and sending of data to fluentd
	hook.mu.Lock()
	defer hook.mu.Unlock()
	// create a connection and retain it
	// note the connection is not closed; if there's an error using the connection closes itself
	// and we'll notice that and reconnect on the next Fire. This reproduces this original library's implementation
	// (ie, a message gets lost)
	if hook.logger == nil {
		var err error
		if hook.logger, err = fluent.New(hook.config); err != nil {
			return err
		}
	}

	err := hook.logger.PostWithTime(tag, entry.Time, fluentData)
	return err
}

func (hook *FluentHook) Levels() []logrus.Level {
	return hook.levels
}

func (hook *FluentHook) SetLevels(levels []logrus.Level) {
	hook.levels = levels
}

func (hook *FluentHook) Tag() string {
	return *hook.tag
}

func (hook *FluentHook) SetTag(tag string) {
	hook.tag = &tag
}
