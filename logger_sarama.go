package kafkax

import (
	"fmt"
	"strings"

	"github.com/IBM/sarama"
)

type saramaLogger struct{}

// NewSaramaLogger create custom logger for "sarama" library for debugging
func NewSaramaLogger() sarama.StdLogger {
	return &saramaLogger{}
}

func (l *saramaLogger) Print(v ...interface{}) {
	fmt.Println("[SARAMA]", v)
}

func (l *saramaLogger) Printf(format string, v ...interface{}) {
	message := strings.Builder{}
	_, _ = fmt.Fprintf(&message, format, v...)

	fmt.Println("[SARAMA]", message.String())
}

func (l *saramaLogger) Println(v ...interface{}) {
	fmt.Println("[SARAMA]", v)
}
