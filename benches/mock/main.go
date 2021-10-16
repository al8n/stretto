package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"
    "fmt"
	"github.com/dgraph-io/ristretto/z"
)

const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

func key() string {
	k := make([]byte, 2)
	for i := range k {
		k[i] = charset[rand.Intn(len(charset))]
	}
	return string(k)
}

type Dataset struct {
	Data []KV `json:"data"`
}

type KV struct {
	Key      string `json:"key"`
	Hash     uint64 `json:"hash"`
	Conflict uint64 `json:"conflict"`
	Val      string `json:"val"`
	Cost     int64  `json:"cost"`
}

func main() {
	var m []KV
	stop := make(chan struct{}, 8)
	fmt.Println("Generating mock data...")
	for i := 0; i < 8; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					time.Sleep(time.Millisecond)

					val := ""
					if rand.Intn(100) < 10 {
						val = "test"
					} else {
						val = strings.Repeat("a", 1000)
					}

					k := key()
					cost := len(val)
					kh, conflict := z.KeyToHash(k)
					m = append(m, KV{k, kh, conflict, val, int64(cost)})
				}
			}
		}()
	}

	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
	}

	for i := 0; i < 8; i++ {
		stop <- struct{}{}
	}

	bytes, err := json.Marshal(Dataset{Data: m})
	if err != nil {
		panic(err)
	}

	err = ioutil.WriteFile("mock.json", bytes, 0644)
	if err != nil {
		panic(err)
	}
}
