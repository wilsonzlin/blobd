package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

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
	started := time.Now()
	for i := uint64(0); i < concurrency; i++ {
		wg.Add(1)
		go func() {
			sess := session.Must(session.NewSession())
			client := s3.New(sess)
			ctx := context.Background()
			for {
				no := atomic.AddUint64(&completed, 1)
				if no > count {
					break
				}
				k := fmt.Sprintf("/random/data/%d", no)
				_, err := client.PutObjectWithContext(ctx, &s3.PutObjectInput{
					Bucket: bucket,
					Key: aws.String(k),
					Body: bytes.NewReader(data),
				})
				if err != nil {
					panic(err)
				}
				rd, err := client.GetObjectWithContext(ctx, &s3.GetObjectInput{
					Bucket: bucket,
					Key: aws.String(k),
				})
				if err != nil {
					panic(err)
				}
				rdBytes, err := io.ReadAll(rd.Body)
				if err != nil {
					panic(err)
				}
				if !bytes.Equal(data, rdBytes) {
					panic(fmt.Errorf("read response does not match data"))
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	durSec := time.Now().Sub(started).Seconds()
	fmt.Printf("Effective time: %f seconds\n", durSec)
	fmt.Printf("Effective processing rate: %f per second\n", float64(count) / durSec)
	fmt.Printf("Effective bandwidth: %f MiB/s\n", float64(count * size) / 1024.0 / 1024.0 / durSec)
}
