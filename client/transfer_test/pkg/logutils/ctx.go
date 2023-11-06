package logutils

import (
	"context"

	"go.uber.org/zap"
)

type ctxKeyTyp int

var ctxKey ctxKeyTyp

func FromContext(ctx context.Context) *zap.Logger {
	v := ctx.Value(ctxKey)
	if v == nil {
		return zap.L()
	}
	return v.(*zap.Logger)
}

func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	ctx = context.WithValue(ctx, ctxKey, logger)
	return ctx
}
