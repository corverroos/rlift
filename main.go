package rlift

import (
	"fmt"
	"strconv"

	lift "github.com/liftbridge-io/go-liftbridge"
	"github.com/luno/jettison/errors"
	"golang.org/x/net/context"
)

func main() {
	// Create Liftbridge client.
	addrs := []string{"localhost:9292", "localhost:9293", "localhost:9294"}
	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create a stream attached to the NATS subject "foo".
	var (
		subject = "foo"
		name    = "foo-stream"
	)
	if err := client.CreateStream(context.Background(), subject, name); err != nil {
		if !errors.Is(err, lift.ErrStreamExists) {
			panic(err)
		}
	}

	go func() {
		for i := 0; i < 100; i++ {
			// Publish a message to "foo".
			if _, err := client.Publish(context.Background(), name, []byte("hello"+strconv.Itoa(i))); err != nil {
				panic(err)
			}
		}
	}()

	// Subscribe to the stream starting from the beginning.
	ctx := context.Background()
	if err := client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset(), string(msg.Value()))
	}, lift.StartAtEarliestReceived()); err != nil {
		panic(err)
	}

	<-ctx.Done()
}
