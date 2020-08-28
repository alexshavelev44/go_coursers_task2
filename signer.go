package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

func ExecutePipeline(hashJobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})

	for _, jobItem := range hashJobs {
		wg.Add(1)
		out := make(chan interface{})
		go func(jobFun job, in chan interface{}, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer close(out)
			jobFun(in, out)
		}(jobItem, in, out, wg)
		in = out // `single hash` out will be `multi hash` in channel
	}

	defer wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	crc32HashChan := make(chan string)

	for val := range in {
		fmt.Println(val, "SingleHash data", val)
		sval := fmt.Sprintf("%v", val)
		md5Val := DataSignerMd5(sval)
		fmt.Println(sval, "SingleHash md5(data)", md5Val)
		wg.Add(1)

		go func(val string, md5Val string, c chan<- string) {
			//defer wg.Done() -- moved to goroutine with context
			crc32ValChan := getCsr32(sval)
			crc32md5ValChan := getCsr32(md5Val)
			crc32Val := <-crc32ValChan
			crc32md5Val := <-crc32md5ValChan
			res := crc32Val + "~" + crc32md5Val
			fmt.Println(sval, "SingleHash crc32(md5(data))", crc32md5Val)
			fmt.Println(sval, "SingleHash crc32(data)", crc32Val)
			fmt.Println(sval, "SingleHash result", res)
			c <- res
		}(sval, md5Val, crc32HashChan)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func(ctx context.Context, o chan<- interface{}, in <-chan string) {

		for {
			select {
			case data := <-in:
				o <- data
				wg.Done()

			case <-ctx.Done():
				return
			}
		}

	}(ctx, out, crc32HashChan)
	wg.Wait()
	cancel()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for val := range in {
		wg.Add(1)
		resChan := make(chan bool)
		sval := fmt.Sprintf("%v", val)
		var result = make([]string, 6, 6)
		for i := 0; i <= 5; i++ {
			go func(i int, data string, c chan<- bool, r []string) {
				dataToHash := strconv.Itoa(i) + sval
				hash := DataSignerCrc32(dataToHash)
				r[i] = hash
				fmt.Println(data, "MultiHash: crc32(th+step1))", i, hash)
				c <- true
			}(i, sval, resChan, result)
		}

		go func(in <-chan bool, n int32) {
			for {
				<-in
				atomic.AddInt32(&n, 1)
				if atomic.LoadInt32(&n) == 6 {
					res := strings.Join(result, "")
					fmt.Println(sval, "MultiHash result:", res)
					out <- res
					wg.Done()
					return
				}
			}
		}(resChan, 0)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var result []string

	for val := range in {
		sval := fmt.Sprintf("%v", val)
		result = append(result, sval)
	}

	sort.Strings(result)
	res := strings.Join(result, "_")
	fmt.Println("CombineResults result:", res)
	out <- res
}

func getCsr32(data string) chan string {
	resChannel := make(chan string)
	go func(c chan<- string, data string) {
		c <- DataSignerCrc32(data)
	}(resChannel, data)
	return resChannel
}
