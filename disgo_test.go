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
	lockConfig := &LockConfig{
		ExpiryTime:         30 * time.Second,
		WaitTime:           10 * time.Second,
		SubscribeSleepTime: 200 * time.Millisecond,
		CasSleepTime:       25 * time.Millisecond,
		SubscribeRatio:     4,
		CasRatio:           1,
	}
	lock, err := GetLock(RDS, lockKey, lockConfig)
	if err != nil {
		fmt.Println(err)
		return
	}
	lockNow := time.Now()
	fmt.Printf("%d, lock start, now: %v, field: %v\n", i, lockNow, lock.distLock.field)
	isSuccess, remark, err := lock.TryLock(ctx)
	if err != nil {
		fmt.Printf("%d, lock err,   now: %v, field: %v, dur: %v, remark: %v, err: %v\n", i, time.Now(), lock.distLock.field, time.Since(lockNow), remark, err)
		return
	}
	if !isSuccess {
		fmt.Printf("%d, lock err,   now: %v, field: %v, dur: %v, remark: %v, err: %v\n", i, time.Now(), lock.distLock.field, time.Since(lockNow), remark, "isSuccess=false")
		return
	}
	defer lock.Release(ctx)

	fmt.Printf("%d, lock wait:  now: %v, field: %v, dur: %v, remark: %v\n", i, time.Now(), lock.distLock.field, time.Since(lockNow), remark)
	time.Sleep(2 * time.Second)
	fmt.Printf("%d, lock end:   now: %v, field: %v, dur: %v, remark: %v\n", i, time.Now(), lock.distLock.field, time.Since(lockNow), remark)
}

//go test -timeout 30s -run ^TestLock$ github.com/TommyLeng/disgo -v -count=1
func TestLock(t *testing.T) {
	_, err := connectRedis("redis://192.168.1.121:6379/0", 2, 4)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			lockProcess(i)
			wg.Done()
		}(i)
		time.Sleep(100 * time.Millisecond)
	}

	wg.Wait()
}
