package kafkax

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/boostgo/convert"
)

const (
	txKey   = "kafka_tx"
	noTxKey = "kafka_no_tx"
)

type Transactor struct {
	producer *SyncProducer
}

func NewTransactor(producer *SyncProducer) *Transactor {
	return &Transactor{
		producer: producer,
	}
}

func (t *Transactor) Key() string {
	return txKey
}

func (t *Transactor) IsTx(ctx context.Context) bool {
	txObject := ctx.Value(txKey)
	if txObject == nil {
		return false
	}

	_, ok := txObject.(*Transaction)
	if !ok {
		return false
	}

	return true
}

func (t *Transactor) Begin(_ context.Context) (*Transaction, error) {
	return newTransaction(t.producer), nil
}

func (t *Transactor) BeginCtx(ctx context.Context) (context.Context, error) {
	tx := newTransaction(t.producer)
	return context.WithValue(ctx, txKey, tx), nil
}

func (t *Transactor) CommitCtx(ctx context.Context) error {
	tx, ok := getTx(ctx)
	if !ok {
		return nil
	}

	return tx.Commit(ctx)
}

func (t *Transactor) RollbackCtx(ctx context.Context) error {
	tx, ok := getTx(ctx)
	if !ok {
		return nil
	}

	return tx.Rollback(ctx)
}

type Transaction struct {
	producer *SyncProducer
	messages []*sarama.ProducerMessage
}

func newTransaction(producer *SyncProducer) *Transaction {
	return &Transaction{
		producer: producer,
		messages: make([]*sarama.ProducerMessage, 0),
	}
}

func (tx *Transaction) addMessages(messages ...*sarama.ProducerMessage) {
	if len(messages) == 0 {
		return
	}

	tx.messages = append(tx.messages, messages...)
}

func (tx *Transaction) Context() context.Context {
	return context.Background()
}

func (tx *Transaction) Commit(ctx context.Context) error {
	if tx.messages == nil {
		return nil
	}

	return tx.producer.Produce(context.Background(), tx.messages...)
}

func (tx *Transaction) Rollback(_ context.Context) error {
	tx.messages = nil
	return nil
}

func NoTxContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, noTxKey, true)
}

func getTx(ctx context.Context) (*Transaction, bool) {
	noTx := ctx.Value(noTxKey)
	if noTx != nil && convert.Bool(noTx) {
		return nil, false
	}

	txObject := ctx.Value(txKey)
	if txObject == nil {
		return nil, false
	}

	tx, ok := txObject.(*Transaction)
	if !ok {
		return nil, false
	}

	return tx, true
}

func addMessagesToTx(tx *Transaction, messages []*sarama.ProducerMessage) bool {
	tx.addMessages(messages...)
	return true
}
