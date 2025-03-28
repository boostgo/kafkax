package kafkax

import (
	"context"
	"github.com/IBM/sarama"
)

type TraceGetter func(ctx context.Context) string
type TraceSetter func(ctx context.Context, traceID string) context.Context
type TraceKafkaGetter func(ctx context.Context, message *sarama.ConsumerMessage) context.Context
type TraceKafkaSetter func(ctx context.Context, messages ...*sarama.ProducerMessage)
type TraceGenerator func(ctx context.Context) string

type Tracer struct {
	master      bool
	getter      TraceGetter
	setter      TraceSetter
	kafkaGetter TraceKafkaGetter
	kafkaSetter TraceKafkaSetter
	generator   TraceGenerator
}

func NewTracer(
	master bool,
	getter TraceGetter,
	setter TraceSetter,
	kafkaGetter TraceKafkaGetter,
	kafkaSetter TraceKafkaSetter,
	generator TraceGenerator,
) *Tracer {
	return &Tracer{
		master:      master,
		getter:      getter,
		setter:      setter,
		kafkaGetter: kafkaGetter,
		kafkaSetter: kafkaSetter,
		generator:   generator,
	}
}

func EmptyTracer() *Tracer {
	return NewTracer(
		false,
		func(ctx context.Context) string {
			return ""
		},
		func(ctx context.Context, traceID string) context.Context {
			return ctx
		},
		func(ctx context.Context, message *sarama.ConsumerMessage) context.Context {
			return ctx
		},
		func(ctx context.Context, messages ...*sarama.ProducerMessage) {},
		func(ctx context.Context) string {
			return ""
		},
	)
}

func (tracer *Tracer) AmIMaster() bool {
	return tracer.master
}

func (tracer *Tracer) Get(ctx context.Context) string {
	return tracer.getter(ctx)
}

func (tracer *Tracer) GetKafka(ctx context.Context, message *sarama.ConsumerMessage) context.Context {
	return tracer.kafkaGetter(ctx, message)
}

func (tracer *Tracer) Set(ctx context.Context, traceID string) context.Context {
	return tracer.setter(ctx, traceID)
}

func (tracer *Tracer) SetKafka(ctx context.Context, messages ...*sarama.ProducerMessage) {
	if len(messages) == 0 {
		return
	}

	tracer.kafkaSetter(ctx, messages...)
}

func (tracer *Tracer) Generate(ctx context.Context) string {
	return tracer.generator(ctx)
}
