package disgo

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v8"
)

var RDS *redis.Client

func connectRedis(dsn string, idle, pool int) (*redis.Client, error) {
	rdsOpts, err := redis.ParseURL(dsn)
	if err != nil {
		return nil, err
	}

	rdsOpts.MinIdleConns = idle
	rdsOpts.PoolSize = pool
	rdsOpts.MaxConnAge = 5 * time.Minute

	rds := redis.NewClient(rdsOpts)
	_, err = rds.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}
	RDS = rds
	return rds, nil
}

func lockProcess(i int) {
	ctx := context.Background()
	lockKey := "TestLockKey"
	lock, err := GetLock(RDS, lockKey)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%d, lock start: %v\n", i, time.Now())
	isSuccess, err := lock.TryLock(ctx, 30*time.Second, 15*time.Second, 25*time.Millisecond)
	if err != nil {
		fmt.Println(err)
		return
	}
	if !isSuccess {
		fmt.Println("isSuccess=false")
		return
	}
	defer lock.Release(ctx)

	fmt.Printf("%d, lock wait : %v\n", i, time.Now())
	time.Sleep(3 * time.Second)
	fmt.Printf("%d, lock end:   %v\n", i, time.Now())
}

func TestLock(t *testing.T) {
	_, err := connectRedis("redis://192.168.1.121:6379/0", 2, 4)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			lockProcess(i)
			wg.Done()
		}(i)
	}

	wg.Wait()
}
