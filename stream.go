package rlift

import (
	"context"
	"strconv"
	"sync"

	lift "github.com/liftbridge-io/go-liftbridge"
	proto "github.com/liftbridge-io/liftbridge-api/go"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
)

func NewStream(cl lift.Client, name string) *Stream {
	return &Stream{
		name:        name,
		natsSubject: "reflex." + name,
		cl:          cl,
	}
}

// Stream represents a liftbridge stream.
type Stream struct {
	// name of the stream.
	name string

	// natsSubject is the underlying nats subject to subscribe to. This may include wildcards.
	natsSubject string

	createOnce sync.Once

	// cl is the lift client.
	cl lift.Client
}

// Stream implements reflex.StreamFunc and returns a StreamClient that
// streams events from liftbridge stream after the provided cursor.
// Stream is safe to call from multiple goroutines, but the returned
// StreamClient is only safe for a single goroutine to use.
func (s *Stream) Stream(ctx context.Context, afterStr string,
	opts ...reflex.StreamOption) (reflex.StreamClient, error) {

	after, err := strconv.ParseInt(afterStr, 10, 64)
	if err != nil {
		return nil, errors.New("only int cursors supported")
	}

	sopts := new(reflex.StreamOptions)
	for _, opt := range opts {
		opt(sopts)
	}

	if sopts.Lag > 0 {
		return nil, errors.New("lag option not supported")
	}

	startOpt := lift.StartAtOffset(after) // TODO(corver): Maybe increment since we want after, not from.
	if sopts.StreamFromHead {
		// This is not actually required, since it is the default.
		startOpt = func(o *lift.SubscriptionOptions) error {
			o.StartPosition = lift.StartPosition(proto.StartPosition_NEW_ONLY)
			return nil
		}
	}

	s.createOnce.Do(func() {
		// TODO(corver): Add support for liftbrigde options.
		err = s.cl.CreateStream(ctx, s.natsSubject, s.name)
	})
	if err != nil {
		return nil, err
	}

	sc := &streamclient{
		eventChan: make(chan *reflex.Event, 1),
		errChan:   make(chan error, 1),
	}
	handler := func(msg *lift.Message, err error) {
		if err != nil {
			sc.errChan <- err
		} else {
			sc.eventChan <- &reflex.Event{
				ID:        strconv.FormatInt(msg.Offset(), 10),
				Type:      partitionType(msg.Partition()), // ?
				ForeignID: string(msg.Key()),
				Timestamp: msg.Timestamp(),
				MetaData:  msg.Value(),
			}
		}
	}

	err = s.cl.Subscribe(ctx, s.name, handler, startOpt)
	if err != nil {
		return nil, err
	}

	return sc, nil
}

type partitionType int32

func (p partitionType) ReflexType() int {
	return int(p)
}

type streamclient struct {
	eventChan chan *reflex.Event
	errChan   chan error
}

func (s *streamclient) Recv() (*reflex.Event, error) {
	select {
	case e := <-s.eventChan:
		return e, nil
	case err := <-s.errChan:
		return nil, err
	}
}
