package main

import (
	"fmt"
	"math/rand"
	"time"
)

const P = 2 // number of producers
const C = 5 // number of consumers
const K = 3 // shared buffer size

const TIME_LIMIT_MS = 100

var produced = 0
var consumed = 0

func produce() int {
	num := rand.Intn(100)
	return num
}

func consume(num int) {
	fmt.Printf("[INFO] Consuming %d...\n", num)
}

func producer(ch chan<- int) {
	for {
		num := produce()
		produced++
		fmt.Printf("[INFO] Produced %d!\n", num)
		ch <- num
	}
}

func consumer(ch <-chan int) {
	for {
		select {
		case num := <-ch:
			consume(num)
			consumed++

		case <-time.After(time.Millisecond * TIME_LIMIT_MS):
			fmt.Printf("[DEBUG] No item has arrived in time. Returning...")
			return
		}
	}
}

func main() {
	ch := make(chan int, K)

	for i := 0; i < P; i++ {
		go producer(ch)
	}

	for i := 0; i < C; i++ {
		go consumer(ch)
	}

	fmt.Printf("[INFO] Produced total: %d", produced)
	fmt.Printf("[INFO] Consumed total: %d", consumed)
	fmt.Printf("[INFO] Are total produced and consumed equal? %t!", produced == consumed)

	select {}
}
