package rlift

import (
	"context"
	"strconv"
	"testing"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

const consumer = "test-stream"

func connect(t *testing.T) lift.Client {
	cl, err := lift.Connect([]string{"localhost:9292"})
	jtest.RequireNil(t, err)

	clean := func() {
		err := cl.DeleteStream(context.TODO(), getStream(consumer))
		if !errors.IsAny(err, lift.ErrNoSuchStream, lift.ErrStreamDeleted) {
			jtest.RequireNil(t, err)
		}
	}

	t.Cleanup(func() {
		clean()
		err = cl.Close()
		jtest.RequireNil(t, err)
	})

	clean()

	return cl
}

func TestIntegration(t *testing.T) {
	ctx := context.Background()
	store := NewCursorStore(connect(t), WithReplicationFactor(1))

	cursor, err := store.GetCursor(ctx, consumer)
	jtest.RequireNil(t, err)
	require.Empty(t, cursor)

	get := func() int {
		cursor, err = store.GetCursor(ctx, consumer)
		jtest.RequireNil(t, err)

		i, err := strconv.Atoi(cursor)
		jtest.RequireNil(t, err)

		return i
	}

	for i := 0; i < 100; i++ {
		c := strconv.Itoa(i)
		err = store.SetCursor(ctx, consumer, c)
		jtest.RequireNil(t, err)
	}

	require.Eventually(t, func() bool {
		return get() == 99
	}, time.Second, time.Millisecond*10)
}
