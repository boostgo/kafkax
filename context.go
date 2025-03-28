package kafkax

import "context"

var AppCtx AppContext

type AppContext struct {
	tears          []func() error
	done           chan struct{}
	gracefulCtx    context.Context
	gracefulCancel context.CancelFunc
}

func NewAppContext(ctx context.Context, cancel context.CancelFunc) AppContext {
	return AppContext{
		tears:          make([]func() error, 0),
		done:           make(chan struct{}, 1),
		gracefulCtx:    ctx,
		gracefulCancel: cancel,
	}
}

func (ctx *AppContext) SetContext(gracefulCtx context.Context, gracefulCancel context.CancelFunc) *AppContext {
	ctx.gracefulCtx = gracefulCtx
	ctx.gracefulCancel = gracefulCancel
	return ctx
}

func (ctx *AppContext) Tear(tear func() error) {
	ctx.tears = append(ctx.tears, tear)
}

func (ctx *AppContext) Done() <-chan struct{} {
	return ctx.gracefulCtx.Done()
}

func (ctx *AppContext) Context() context.Context {
	return ctx.gracefulCtx
}

func (ctx *AppContext) Cancel() {
	ctx.gracefulCancel()
}
