package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

// A list of task types.
const (
	TypeTest1 = "test:test1"

	defaultRedisAddress        = "127.0.0.1:6379"
	defaultNumberOfJobs        = 1
	defaultConsumerConcurrency = 2048
)

var (
	redisAddress        = flag.String("r", defaultRedisAddress, "Redis server address, defaults to 127.0.0.1:6379")
	numJobs             = flag.Int("j", defaultNumberOfJobs, "Number of numJto submit, defaults to 1")
	consumerConcurrency = flag.Int("cc", defaultConsumerConcurrency, "Number of concurrent consumers, defaults to 2048")
)

type Test1Payload struct {
	ID int
}

func NewTest1Task(id int) (*asynq.Task, error) {
	payload, err := json.Marshal(Test1Payload{ID: id})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeTest1, payload), nil
}

func HandleTest1Task(ctx context.Context, t *asynq.Task) error {
	var p Test1Payload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("dequeue: json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	log.Printf("dequeue: processed %#v", p)
	return nil
}

func main() {
	flag.Parse()

	go func() { // Start a concurrent task consumer.
		srv := asynq.NewServer(
			asynq.RedisClientOpt{Addr: *redisAddress},
			asynq.Config{
				// Specify how many concurrent workers to use
				Concurrency: *consumerConcurrency,
				LogLevel:    asynq.DebugLevel,
			},
		)

		mux := asynq.NewServeMux()
		mux.HandleFunc(TypeTest1, HandleTest1Task)
		if err := srv.Run(mux); err != nil {
			log.Fatalf("could not run server: %v", err)
		}
	}()

	// Create a client and sibmit a job.
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: *redisAddress})
	defer client.Close()

	for i := 0; i < *numJobs; i++ {
		task, err := NewTest1Task(i)
		if err != nil {
			log.Fatalf("enqueue: could not create task: %v", err)
		}

		info, err := client.Enqueue(task)
		if err != nil {
			log.Fatalf("enqueue: could not enqueue task: %v", err)
		}
		log.Printf("enqueue: task id=%s queue=%s", info.ID, info.Queue)
	}

	time.Sleep(3 * time.Second)
}
