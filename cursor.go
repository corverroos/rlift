package rlift

import (
	"context"
	"sync"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

const (
	streamPrefix      = "cursors."
	subjectPrefix     = "reflex.cursors."
	replicationFactor = 3

	// errNoKnownStream is sometimes returned by the server (instead of lift.ErrNoSuchPartition)
	errNoKnownStream = "no known stream"
)

var (
	// notFoundTimeout defines the timeout when getting a cursor to decide there are no cursors in the stream.
	// This might happen when a stream is created but no cursor is actually stored.
	notFoundTimeout = time.Second * 15
)

type Option func(*CursorStore)

func WithReplicationFactor(n int) Option {
	return func(s *CursorStore) {
		s.replicationFactor = int32(n)
	}
}

func WithNotFoundTimeout(d time.Duration) Option {
	return func(s *CursorStore) {
		s.notFoundTimeout = d
	}
}

func NewCursorStore(cl lift.Client, opts ...Option) *CursorStore {
	s := &CursorStore{
		cl:                cl,
		consumers:         make(map[string]bool),
		notFoundTimeout:   notFoundTimeout,
		replicationFactor: replicationFactor,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// CursorStore implements the reflex.CursorStore interface and provides
// a cursor stored backed by a lift stream per consumer. It
// sets the message key to the consumer name, so each stream
// compacts to the last cursor.
type CursorStore struct {
	cl        lift.Client
	consumers map[string]bool
	mu        sync.Mutex

	notFoundTimeout   time.Duration
	replicationFactor int32
}

func (s *CursorStore) GetCursor(ctx context.Context, consumerName string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.notFoundTimeout)
	defer cancel()

	var (
		err    error
		cursor string
	)
	handler := func(msg *lift.Message, streamErr error) {
		if streamErr != nil {
			err = streamErr
			cancel()
			return
		}
		if string(msg.Key()) != consumerName {
			err = errors.New("unexpected msg key",
				j.MKS{"actual": string(msg.Key()), "expected": consumerName})
		}
		cursor = string(msg.Value())
		cancel()
	}

	err = s.cl.Subscribe(ctx, getStream(consumerName), handler,
		lift.ReadISRReplica(),
		lift.StartAtLatestReceived(),
	)
	if errors.IsAny(err, lift.ErrNoSuchPartition) {
		return "", nil
	} else if err != nil && err.Error() == errNoKnownStream {
		return "", nil
	} else if err != nil {
		return "", err
	}

	<-ctx.Done()

	if err != nil {
		return "", err
	} else if cursor != "" {
		return cursor, nil
	}

	err = ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) {
		return "", nil
	}

	// We should never reach this.
	return "", errors.Wrap(err, "expected context error (bug)")
}

func (s *CursorStore) SetCursor(ctx context.Context, consumerName string, cursor string) error {
	if err := s.Init(ctx, consumerName); err != nil {
		return err
	}

	_, err := s.cl.Publish(ctx, getStream(consumerName), []byte(cursor),
		lift.Key([]byte(consumerName)),
		lift.AckPolicyNone(),
	)

	return err
}

func (s *CursorStore) Flush(ctx context.Context) error {
	// TODO(corver): Maybe implement buffering to decrease network io.
	return nil
}

func (s *CursorStore) Init(ctx context.Context, consumerName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.consumers[consumerName]; ok {
		return nil
	}

	err := s.cl.CreateStream(ctx, getSubject(consumerName), getStream(consumerName),
		lift.ReplicationFactor(s.replicationFactor),
	)
	if errors.Is(err, lift.ErrStreamExists) {
		// Happy :)
	} else if err != nil {
		return err
	}

	s.consumers[consumerName] = true

	return nil
}

func getStream(consumerName string) string {
	return streamPrefix + consumerName
}

func getSubject(consumerName string) string {
	return subjectPrefix + consumerName
}
