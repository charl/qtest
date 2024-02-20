package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mistsys/mist_go_utils/uuid"
	"golang.org/x/sync/errgroup"
)

// A list of task types.
const (
	defaultRedisAddress        = "127.0.0.1:6379"
	defaultNumberOfJobs        = 1
	defaultConsumerConcurrency = 2048
	key                        = "list:wq"
)

var (
	redisAddress = flag.String("r", defaultRedisAddress, "Redis server address, defaults to 127.0.0.1:6379")
	numJobs      = flag.Int("j", defaultNumberOfJobs, "Number of numJto submit, defaults to 1")
	// consumerConcurrency = flag.Int("cc", defaultConsumerConcurrency, "Number of concurrent consumers, defaults to 2048")
	redisClusterPool *redis.Pool
)

type OrgTask struct {
	OrgID   uuid.TimeUUID
	Devices int
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, _ := errgroup.WithContext(ctx)

	rdb, err := createRedisPool(*redisAddress, 5, 10)
	if err != nil {
		panic(err)
	}
	defer rdb.Close()

	g.Go(func() error {
		return produce(rdb)
	})

	g.Go(func() error {
		return consume(rdb)
	})

	err = g.Wait() // Wait for all services to exit.
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[ERRGROUP] %v", err)
	}
}

func createRedisPool(addr string, idle, active int, opts ...redis.DialOption) (*redis.Pool, error) {
	return &redis.Pool{
		MaxIdle:     idle,
		MaxActive:   active,
		IdleTimeout: time.Minute,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}

			_, err := c.Do("PING")
			return err
		},
	}, nil
}

func parseRedisClusterMember(err string) string {
	// MOVED 12359 10.1.20.65:6379
	return err[strings.LastIndex(err, " ")+1:]
}

func produceJob(rdb *redis.Pool, task OrgTask) error {
	conn := rdb.Get()
	defer conn.Close()

	j, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("enqueue: cannot marshal job to json: %v", err)
	}

	_, err = conn.Do("LPUSH", key, j)
	if err != nil {
		return fmt.Errorf("enqueue: cannot enqueue job: %v", err)
	}

	return nil
}

func produce(rdb *redis.Pool) error {
	for {
		for i := 0; i < *numJobs; i++ {
			task := OrgTask{
				OrgID:   uuid.UUID1(time.Now().UnixNano()),
				Devices: i,
			}
			err := produceJob(rdb, task)
			if err != nil {
				return err
			}

			log.Printf("enqueue: job: %d", i)
		}

		time.Sleep(10 * time.Second)
	}
}

func consume(rdb *redis.Pool) error {
	conn := rdb.Get()
	defer conn.Close()

	for {
		res, err := redis.Strings(conn.Do("BRPOP", key, 0))
		log.Printf(">>>> res: %#v, err: %#v", res, err)

		if err != nil && strings.HasPrefix(err.Error(), "MOVED ") {
			member := parseRedisClusterMember(err.Error())
			log.Printf(">>>> MOVED: %s: res: %#v, err: %#v", member, res, err)
			rdb2, err := createRedisPool(member, 1, 1)
			if err != nil {
				log.Printf("dequeue: cannot pop job after moved: %s: %v", member, err)
				continue
			}

			conn2 := rdb2.Get()
			res, err = redis.Strings(conn2.Do("BLPOP", key, 0))

			conn2.Close()
			rdb2.Close()

			log.Printf(">>>> AFTER MOVED: %s: res: %#v, err: %#v", member, res, err)
		}

		if err != nil {
			if err != nil {
				log.Printf("dequeue: cannot pop job: %v", err)
				continue
			}
		}

		if len(res) != 2 {
			log.Printf("dequeue: cannot pop job, invalid response: %v", res)
			continue
		}

		var j OrgTask
		err = json.Unmarshal([]byte(res[1]), &j)
		if err != nil {
			log.Printf("dequeue: cannot pop job: %v", err)
			continue
		}

		log.Printf("dequeue: completed job: %#v", j)
	}
}
