package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/klauspost/cpuid/v2"
)

type Op struct {
	PutObjectMs int64
	GetObjectResponseMs int64
	GetObjectTransferMs int64
}

type Results struct {
	CpuCount uint64
	CpuModel string
	Concurrency uint64
	Count uint64
	Size uint64
	Ops []Op
}

func main() {
	bucket := aws.String(os.Getenv("BUCKET"))
	concurrency, err := strconv.ParseUint(os.Getenv("CONCURRENCY"), 10, 64)
	if err != nil {
		panic(err)
	}
	count, err := strconv.ParseUint(os.Getenv("COUNT"), 10, 64)
	if err != nil {
		panic(err)
	}
	size, err := strconv.ParseUint(os.Getenv("SIZE"), 10, 64)
	if err != nil {
		panic(err)
	}
	data := make([]byte, size)
	_, err = rand.Read(data)
	if err != nil {
		panic(err)
	}
	completed := uint64(0)
	var wg sync.WaitGroup
	results := Results{
		CpuCount: uint64(runtime.NumCPU()),
		CpuModel: CPU.BrandName,
		Concurrency: concurrency,
		Count: count,
		Size: size,
		Ops: make([]Op, count),
	}
	for i := uint64(0); i < concurrency; i++ {
		wg.Add(1)
		go func() {
			sess := session.Must(session.NewSession())
			client := s3.New(sess)
			ctx := context.Background()
			for {
				no := atomic.AddUint64(&completed, 1) - 1
				if no >= count {
					break
				}
				k := fmt.Sprintf("/random/data/%d", no)
				putStarted := time.Now()
				_, err := client.PutObjectWithContext(ctx, &s3.PutObjectInput{
					Bucket: bucket,
					Key: aws.String(k),
					Body: bytes.NewReader(data),
				})
				if err != nil {
					panic(err)
				}
				results.Ops[no].PutObjectMs = time.Since(putStarted).Milliseconds()
				getStarted := time.Now()
				rd, err := client.GetObjectWithContext(ctx, &s3.GetObjectInput{
					Bucket: bucket,
					Key: aws.String(k),
				})
				if err != nil {
					panic(err)
				}
				results.Ops[no].GetObjectResponseMs = time.Since(getStarted).Milliseconds()
				getReadStarted := time.Now()
				_, err = io.ReadAll(rd.Body)
				if err != nil {
					panic(err)
				}
				results.Ops[no].GetObjectTransferMs = time.Since(getReadStarted).Milliseconds()
			}
			wg.Done()
		}()
	}
	wg.Wait()

	resultsJson, err := json.Marshal(results)
	if err != nil {
		panic(err)
	}
	os.Stdout.Write(resultsJson)
}
