package kafkax

import (
	"context"

	"github.com/IBM/sarama"
)

// AsyncProducer producer which produce messages in "async" way
type AsyncProducer struct {
	producer sarama.AsyncProducer
	tracer   *Tracer
}

// AsyncProducerOption returns default async producer configuration as [Option]
func AsyncProducerOption() Option {
	return func(config *sarama.Config) {
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true
	}
}

// NewAsyncProducer creates [AsyncProducer] with configurations.
//
// Creates async producer with default configuration as [Option] created by [AsyncProducerOption] function.
//
// Adds producer close method to teardown
func NewAsyncProducer(cfg Config, opts ...Option) (*AsyncProducer, error) {
	config := sarama.NewConfig()
	config.ClientID = buildClientID()
	AsyncProducerOption()(config)

	for _, opt := range opts {
		opt(config)
	}

	producer, err := sarama.NewAsyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, err
	}

	return &AsyncProducer{
		producer: producer,
		tracer:   cfg.GetTracer(),
	}, nil
}

// NewAsyncProducerFromClient creates [AsyncProducer] by provided client.
//
// Creates async producer with default configuration as [Option] created by [AsyncProducerOption] function.
//
// Adds producer close method to teardown
func NewAsyncProducerFromClient(client sarama.Client, tracer *Tracer) (*AsyncProducer, error) {
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	if tracer == nil {
		tracer = EmptyTracer()
	}

	return &AsyncProducer{
		producer: producer,
		tracer:   tracer,
	}, nil
}

// MustAsyncProducer calls [NewAsyncProducer] function with calls panic if returns error
func MustAsyncProducer(cfg Config, opts ...Option) *AsyncProducer {
	producer, err := NewAsyncProducer(cfg, opts...)
	if err != nil {
		panic(err)
	}

	return producer
}

// MustAsyncProducerFromClient calls [NewAsyncProducerFromClient] function with calls panic if returns error
func MustAsyncProducerFromClient(client sarama.Client, tracer *Tracer) *AsyncProducer {
	producer, err := NewAsyncProducerFromClient(client, tracer)
	if err != nil {
		panic(err)
	}

	return producer
}

// Produce sends provided message(s) in other goroutine.
//
// Sets trace id to provided messages to header
func (producer *AsyncProducer) Produce(ctx context.Context, messages ...*sarama.ProducerMessage) error {
	if len(messages) == 0 {
		return nil
	}

	if producer.tracer.AmIMaster() && producer.tracer.Get(ctx) == "" {
		ctx = producer.tracer.Set(ctx, producer.tracer.Generate(ctx))
	}

	producer.tracer.SetKafka(ctx, messages...)

	for _, msg := range messages {
		producer.producer.Input() <- msg
	}

	return nil
}
