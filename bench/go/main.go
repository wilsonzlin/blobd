package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/klauspost/cpuid/v2"
)

type TurbostoreError struct {
	method string
	code byte
}

func (e TurbostoreError) Error() string {
	var eng string
	switch (e.code) {
	case 1:
		eng = "not enough args"
	case 2:
		eng = "key is too long"
	case 3:
		eng = "too many args"
	case 4:
		eng = "not found"
	case 5:
		eng = "invalid start"
	case 6:
		eng = "invalid end"
	case 7:
		panic(fmt.Errorf("invalid error code %d", e.code))
	}
	return fmt.Sprintf("%s request encountered error: %s", e.method, eng)
}

type TurbostoreClient struct {
	conn *net.TCPConn
	buf []byte
	busy bool
}

func NewTurbostoreClient(host string) (*TurbostoreClient, error) {
	addr, err := net.ResolveTCPAddr("tcp", host + ":9001")
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	client := &TurbostoreClient{
		conn: conn,
		buf: make([]byte, 64 * 1024),
		busy: false,
	}
	return client, nil
}

func (c *TurbostoreClient) markAsBusy() {
	if c.busy {
		panic("Client is busy")
	}
	c.busy = true
}

func (c *TurbostoreClient) markAsFree() {
	if !c.busy {
		panic("Client is not busy")
	}
	c.busy = false
}

func (c *TurbostoreClient) writeRequest() error {
	writeno, err := c.conn.Write(c.buf[0:255])
	if err != nil {
		return err
	}
	if writeno != 255 {
		return errors.New("not enough bytes written")
	}
	return nil
}

func (c *TurbostoreClient) readAndCheckResponse(method string, len int) error {
	readno, err := c.conn.Read(c.buf[0:len])
	if err != nil {
		return err
	}
	if readno != len {
		return errors.New("not enough bytes read")
	}
	if c.buf[0] != 0 {
		return TurbostoreError{
			method: method,
			code: c.buf[0],
		}
	}
	return nil
}

func (c *TurbostoreClient) CommitObject(inodeDevOffset uint64, objNo uint64) error {
	c.markAsBusy()
	defer c.markAsFree()
	c.buf[0] = 5
	binary.BigEndian.PutUint64(c.buf[1:], inodeDevOffset)
	binary.BigEndian.PutUint64(c.buf[9:], objNo)
	c.writeRequest()
	err := c.readAndCheckResponse("commit_object", 1)
	if err != nil {
		return err
	}
	return nil
}

type TurbostoreCreateObjectResponse struct {
	inodeDeviceOffset uint64
	objectNumber uint64
}

func (c *TurbostoreClient) CreateObject(key string, size uint64) (*TurbostoreCreateObjectResponse, error) {
	c.markAsBusy()
	defer c.markAsFree()
	c.buf[0] = 1
	c.buf[1] = byte(len(key))
	copy(c.buf[2:2+len(key)], key)
	binary.BigEndian.PutUint64(c.buf[2+len(key):], size)
	c.writeRequest()
	err := c.readAndCheckResponse("create_object", 17)
	if err != nil {
		return nil, err
	}
	return &TurbostoreCreateObjectResponse{
		inodeDeviceOffset: binary.BigEndian.Uint64(c.buf[1:]),
		objectNumber: binary.BigEndian.Uint64(c.buf[9:]),
	}, nil
}

func (c *TurbostoreClient) DeleteObjectWithNumber(key string, objNo uint64) error {
	c.markAsBusy()
	defer c.markAsFree()
	c.buf[0] = 6
	c.buf[1] = byte(len(key))
	copy(c.buf[2:2+len(key)], key)
	binary.BigEndian.PutUint64(c.buf[2+len(key):], objNo)
	c.writeRequest()
	err := c.readAndCheckResponse("delete_object", 1)
	if err != nil {
		return err
	}
	return nil
}

func (c *TurbostoreClient) DeleteObject(key string) error {
	return c.DeleteObjectWithNumber(key, 0)
}

type TurbostoreInspectObjectResponse struct {
	size uint64
}

func (c *TurbostoreClient) InspectObject(key string) (*TurbostoreInspectObjectResponse, error) {
	c.markAsBusy()
	defer c.markAsFree()
	c.buf[0] = 2
	c.buf[1] = byte(len(key))
	copy(c.buf[2:2+len(key)], key)
	c.writeRequest()
	err := c.readAndCheckResponse("inspect_object", 10)
	if err != nil {
		return nil, err
	}
	return &TurbostoreInspectObjectResponse{
		size: binary.BigEndian.Uint64(c.buf[2:]),
	}, nil
}

type TurbostoreReadObjectResponse struct {
	actualStart uint64
	actualLength uint64
	objectSize uint64
	didRead uint64
	c *TurbostoreClient
}

func (r *TurbostoreReadObjectResponse) Read(b []byte) (int, error) {
	maxRead := r.actualLength - r.didRead
	if maxRead > uint64(len(b)) {
		maxRead = uint64(len(b))
	}
	readno, err := r.c.conn.Read(b[0:maxRead])
	r.didRead += uint64(readno)
	if r.didRead == r.actualLength {
		defer r.c.markAsFree()
		// TODO Check if err == nil?
		return readno, io.EOF
	}
	return readno, err
}

func (c *TurbostoreClient) ReadObject(key string, start, end int64) (*TurbostoreReadObjectResponse, error) {
	c.markAsBusy()
	c.buf[0] = 3
	c.buf[1] = byte(len(key))
	copy(c.buf[2:2+len(key)], key)
	binary.BigEndian.PutUint64(c.buf[2+len(key):], uint64(start))
	binary.BigEndian.PutUint64(c.buf[2+len(key)+8:], uint64(end))
	c.writeRequest()
	err := c.readAndCheckResponse("read_object", 25)
	if err != nil {
		c.markAsFree()
		return nil, err
	}
	actualStart := binary.BigEndian.Uint64(c.buf[1:])
	actualLength := binary.BigEndian.Uint64(c.buf[9:])
	objectSize := binary.BigEndian.Uint64(c.buf[17:])
	return &TurbostoreReadObjectResponse{
		actualStart: actualStart,
		actualLength: actualLength,
		objectSize: objectSize,
		didRead: 0,
		c: c,
	}, nil
}

type TurbostoreWriteObjectResponse struct {
	didWrite uint64
	len uint64
	c *TurbostoreClient
}

func (r *TurbostoreWriteObjectResponse) Write(b []byte) (int, error) {
	if r.didWrite + uint64(len(b)) > r.len {
		panic("Attempted to write too many bytes")
	}
	writeno, err := r.c.conn.Write(b)
	r.didWrite += uint64(writeno)
	if r.didWrite == r.len {
		defer r.c.markAsFree()
		err := r.c.readAndCheckResponse("write_object", 1)
		if err != nil {
			return writeno, err
		}
	}
	return writeno, err
}

func (c *TurbostoreClient) WriteObject(inodeDevOffset, objNo, start, len uint64) (*TurbostoreWriteObjectResponse, error) {
	c.markAsBusy()
	c.buf[0] = 4
	binary.BigEndian.PutUint64(c.buf[1:], inodeDevOffset)
	binary.BigEndian.PutUint64(c.buf[9:], objNo)
	binary.BigEndian.PutUint64(c.buf[17:], start)
	binary.BigEndian.PutUint64(c.buf[25:], len)
	c.writeRequest()
	return &TurbostoreWriteObjectResponse{
		didWrite: 0,
		len: len,
		c: c,
	}, nil
}

type Op struct {
	CreateObjectResponseMs int64
	WriteObjectResponseMs int64
	WriteObjectTransferMs int64
	CommitObjectResponseMs int64
	ReadObjectResponseMs int64
	ReadObjectTransferMs int64
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
			client, err := NewTurbostoreClient(os.Getenv("TS_HOSTNAME"))
			if err != nil {
				panic(err)
			}
			for {
				no := atomic.AddUint64(&completed, 1) - 1
				if no >= count {
					break
				}
				k := fmt.Sprintf("/random/data/%d", no)

				crStarted := time.Now()
				cr, err := client.CreateObject(k, size)
				if err != nil {
					panic(err)
				}
				results.Ops[no].CreateObjectResponseMs = time.Since(crStarted).Milliseconds()

				wrStarted := time.Now()
				wr, err := client.WriteObject(cr.inodeDeviceOffset, cr.objectNumber, 0, size)
				if err != nil {
					panic(err)
				}
				results.Ops[no].WriteObjectResponseMs = time.Since(wrStarted).Milliseconds()

				wrTxStarted := time.Now()
				writeno, err := wr.Write(data)
				if err != nil {
					panic(err)
				}
				if uint64(writeno) != size {
					panic(fmt.Errorf("expected write of %d bytes but wrote %d", size, writeno))
				}
				results.Ops[no].WriteObjectTransferMs = time.Since(wrTxStarted).Milliseconds()

				comStarted := time.Now()
				err = client.CommitObject(cr.inodeDeviceOffset, cr.objectNumber)
				if err != nil {
					panic(err)
				}
				results.Ops[no].CommitObjectResponseMs = time.Since(comStarted).Milliseconds()

				rdStarted := time.Now()
				rd, err := client.ReadObject(k, 0, int64(size))
				if err != nil {
					panic(err)
				}
				results.Ops[no].ReadObjectResponseMs = time.Since(rdStarted).Milliseconds()

				rdTxStarted := time.Now()
				_, err = io.ReadAll(rd)
				if err != nil {
					panic(err)
				}
				results.Ops[no].ReadObjectTransferMs = time.Since(rdTxStarted).Milliseconds()
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
