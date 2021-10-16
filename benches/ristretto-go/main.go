package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/dgraph-io/ristretto"
)

type Dataset struct {
	Data []KV `json:"data"`
}

type KV struct {
	Key  string `json:"key"`
	Hash uint64 `json:"hash"`
	Conflict uint64 `json:"conflict"`
	Val  string `json:"val"`
	Cost int64  `json:"cost"`
}

type KC struct {
	hash uint64
	conflict uint64
}

func main() {
	content, err := ioutil.ReadFile("mock.json")
	if err != nil {
		panic(err)
	}

	var dataset Dataset

	err = json.Unmarshal(content, &dataset)
	if err != nil {
		panic(err)
	}

	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 12960, // 36^2 * 10
		MaxCost:     1e6,   // 1mb
		BufferItems: 64,
		Metrics:     true,
		KeyToHash: func(key interface{}) (uint64, uint64) {
			kc := key.(KC)
			return kc.hash, kc.conflict
		},
	})

	if err != nil {
		panic(err)
	}

	t := time.Now()
	for _, kv := range dataset.Data {
		kc := KC{
			hash: kv.Hash,
			conflict: kv.Conflict,
		}
		if _, ok := c.Get(kc); !ok {

			c.Set(kc, kv.Val, kv.Cost)
		}
	}
	elapsed := time.Since(t)
	fmt.Printf("---Go Ristretto Finished in %dms---\n", elapsed.Milliseconds())
	fmt.Println(c.Metrics.String())
}
