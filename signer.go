package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

const th = 6

func ExecutePipeline(hashSignJobs ...job) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	in := make(chan interface{})
	for _, jobFunc := range hashSignJobs {
		wg.Add(1)
		out := make(chan interface{})
		go func(jobFunc job, in chan interface{}, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer close(out)
			jobFunc(in, out)
		}(jobFunc, in, out, wg)
		in = out
	}
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	//fmt.Println("Start singleHash")
	for i := range in {
		data := fmt.Sprintf("%v", i)
		//fmt.Println("SingleHash data: ", data)
		crcMd5 := DataSignerMd5(data)
		wg.Add(1)
		go workerSingleHash( data, crcMd5, out, wg)
	}
}

func workerSingleHash( data, crcMd5 string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	crc32Chan := make(chan string)
	crcMd5Chan := make(chan string)

	go calcHashDataSignerCrc32(data, crc32Chan)
	left := <-crc32Chan
	go calcHashDataSignerCrc32(crcMd5, crcMd5Chan)
	right := <-crcMd5Chan

	//fmt.Println("workerSingleHash res:",left + "~" + right)
	out <- left + "~" + right
}


func calcHashDataSignerCrc32(data string, out chan string) {
	res := DataSignerCrc32(data)
	out <- res
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	for i := range in {
		wg.Add(1)
		go workerMultiHash(i, out, wg)
	}

}

func workerMultiHash(i interface{}, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	wg2 := &sync.WaitGroup{}
	strSlice := make([]string, th)

	for j := 0; j < th; j++ {
		data := fmt.Sprintf("%v%v", j, i)
		wg2.Add(1)
		go calcMultiHash(data, j, strSlice, wg2)
	}
	wg2.Wait()
	res := strings.Join(strSlice,"")
	//fmt.Println("workerMultiHash  res:", res)
	out <- res
}

func calcMultiHash(data string, j int,strSlice []string, wg2 *sync.WaitGroup) {
	defer wg2.Done()
	res := DataSignerCrc32(data)
	strSlice[j] = res
}

func CombineResults(in chan interface{}, out chan interface{}) {
	var strSlice []string
	for i := range in {
		//fmt.Println("CombineResults i:", i.(string))
		strSlice = append(strSlice, i.(string))
	}
	sort.Strings(strSlice)
	res := strings.Join(strSlice, "_")
	//fmt.Println("CombineResults res:", res)
	out <- res
}




