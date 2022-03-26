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
	CreateObjectResponseNs int64
	WriteObjectNs int64
	CommitObjectResponseNs int64
	ReadObjectResponseNs int64
	ReadObjectTransferNs int64
}

type Results struct {
	TotalTimeNs uint64
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
	started := time.Now()
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

				crStarted := time.Now()
				cr, err := client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
					Bucket: bucket,
					Key: &k,
				})
				if err != nil {
					panic(err)
				}
				results.Ops[no].CreateObjectResponseNs = time.Since(crStarted).Nanoseconds()

				wrStarted := time.Now()
				wr, err := client.UploadPartWithContext(ctx, &s3.UploadPartInput{
					Bucket: bucket,
					Key: &k,
					UploadId: cr.UploadId,
					Body: bytes.NewReader(data),
				})
				if err != nil {
					panic(err)
				}
				results.Ops[no].WriteObjectNs = time.Since(wrStarted).Nanoseconds()

				comStarted := time.Now()
				partNo := int64(1)
				_, err = client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
					Bucket: bucket,
					Key: &k,
					MultipartUpload: &s3.CompletedMultipartUpload{
						Parts: []*s3.CompletedPart{
							&s3.CompletedPart{
								ETag: wr.ETag,
								PartNumber: &partNo,
							},
						},
					},
					UploadId: cr.UploadId,
				})
				if err != nil {
					panic(err)
				}
				results.Ops[no].CommitObjectResponseNs = time.Since(comStarted).Nanoseconds()

				rdStarted := time.Now()
				rd, err := client.GetObjectWithContext(ctx, &s3.GetObjectInput{
					Bucket: bucket,
					Key: &k,
				})
				if err != nil {
					panic(err)
				}
				results.Ops[no].ReadObjectResponseNs = time.Since(rdStarted).Milliseconds()

				rdTxStarted := time.Now()
				_, err = io.ReadAll(rd.Body)
				if err != nil {
					panic(err)
				}
				results.Ops[no].ReadObjectTransferNs = time.Since(rdTxStarted).Milliseconds()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	results.TotalTimeNs = uint64(time.Since(started).Nanoseconds())

	resultsJson, err := json.Marshal(results)
	if err != nil {
		panic(err)
	}
	os.Stdout.Write(resultsJson)
}
