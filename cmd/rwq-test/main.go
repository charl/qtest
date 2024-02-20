package main

import (
	"context"
	"flag"
	"log"
	"time"

	wq "github.com/mevitae/redis-work-queue/go"
	"github.com/redis/go-redis/v9"
)

// A list of task types.
const (
	defaultRedisAddress        = "127.0.0.1:6379"
	defaultNumberOfJobs        = 1
	defaultConsumerConcurrency = 2048
)

var (
	redisAddress        = flag.String("r", defaultRedisAddress, "Redis server address, defaults to 127.0.0.1:6379")
	numJobs             = flag.Int("j", defaultNumberOfJobs, "Number of numJto submit, defaults to 1")
	consumerConcurrency = flag.Int("cc", defaultConsumerConcurrency, "Number of concurrent consumers, defaults to 2048")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddress,
		PoolSize: 8,
	})
	q := wq.NewWorkQueue(wq.KeyPrefix("rwq"))

	for i := 0; i < *consumerConcurrency; i++ { // Start a concurrent task consumer.
		go func(ctx context.Context, id int) {
			for {
				job, err := q.Lease(ctx, rdb, true, 10*time.Second, 5*time.Second)
				if err != nil {
					panic(err)
				}
				// doSomeWork(job)
				_, err = q.Complete(ctx, rdb, job)
				if err != nil {
					log.Printf("dequeue %d: cannot complete item: %s", id, err)
				}
				log.Printf("dequeue %d: completed item: %#v", id, job)
			}
		}(ctx, i+1)
	}

	for i := 0; i < *numJobs; i++ {
		jsonItem, err := wq.NewItemFromJSONData([]int{i})
		if err != nil {
			log.Fatalf("enqueue: could not create item: %v", err)
		}

		err = q.AddItem(ctx, rdb, jsonItem)
		if err != nil {
			log.Fatalf("enqueue: could not enqueue item: %v", err)
		}
	}

	time.Sleep(3 * time.Second)
}
