package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	serverAddr = "localhost:9080"
	apiKey     = "test"
	numWorkers = 1000
)

var sentCount atomic.Int64

func main() {
	frames := flag.Int("frames", 1_000_000, "total number of frames to send")
	flag.Parse()

	remaining := atomic.Int64{}
	remaining.Store(int64(*frames))

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			fmt.Printf("throughput: %d frames/sec\n", sentCount.Swap(0))
		}
	}()

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numWorkers; i++ {
		deviceId := uint64(i + 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var conn net.Conn
			defer func() {
				if conn != nil {
					conn.Close()
				}
			}()
			for {
				if remaining.Add(-1) < 0 {
					return
				}
				if conn == nil {
					conn = dial()
				}
				metricType := uint16(rand.Intn(3) + 1)
				value := rand.Float32() * 1000
				if err := sendFrame(conn, apiKey, deviceId, metricType, value); err != nil {
					conn.Close()
					conn = nil
				} else {
					sentCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	total := int64(*frames)
	fmt.Printf("sent %d frames in %s (%.0f frames/sec)\n", total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
}

func dial() net.Conn {
	for {
		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			fmt.Println("reconnecting:", err)
			time.Sleep(time.Second)
			continue
		}
		return conn
	}
}

func writeUTF(buf []byte, offset int, s string) int {
	b := []byte(s)
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(b)))
	copy(buf[offset+2:], b)
	return 2 + len(b)
}

func sendFrame(conn net.Conn, key string, deviceId uint64, metricType uint16, value float32) error {
	keyBytes := []byte(key)
	payloadLen := 2 + len(keyBytes) + 8 + 8 + 2 + 4
	buf := make([]byte, payloadLen)

	offset := 0
	offset += writeUTF(buf, offset, key)
	binary.BigEndian.PutUint64(buf[offset:], deviceId)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(time.Now().UnixMilli()))
	offset += 8
	binary.BigEndian.PutUint16(buf[offset:], metricType)
	offset += 2
	binary.BigEndian.PutUint32(buf[offset:], math.Float32bits(value))

	_, err := conn.Write(buf)
	return err
}