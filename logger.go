package kafkax

import (
	"context"
	"fmt"
)

type LogArg struct {
	Type  string
	Name  string
	Value any
}

func NewLogArg(argType, name string, value any) LogArg {
	return LogArg{
		Type:  argType,
		Name:  name,
		Value: value,
	}
}

type Logger interface {
	Print(ctx context.Context, level, message string, args ...LogArg)
}

type kafkaLogger struct {
	activated bool
}

func NewLogger(activated bool) Logger {
	return &kafkaLogger{
		activated: activated,
	}
}

func (logger *kafkaLogger) Activate(activate bool) Logger {
	logger.activated = activate
	return logger
}

func (logger *kafkaLogger) Print(
	_ context.Context,
	level, message string,
	args ...LogArg,
) {
	if !logger.activated {
		return
	}

	stringArgs := make([]string, 0, len(args))
	for _, arg := range args {
		stringArgs = append(stringArgs, fmt.Sprintf("%s=%s", arg.Type, arg.Value))
	}

	fmt.Printf("[%s] %s %s\n", level, message, stringArgs)
}
