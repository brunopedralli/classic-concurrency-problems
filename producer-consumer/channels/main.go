package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const P = 2 // number of producers
const C = 5 // number of consumers
const K = 3 // shared buffer size

const PRODUCERS_TIME_LIMIT_S = 5
const CONSUMERS_TIME_LIMIT_MS = 100

var produced = 0
var consumed = 0

var closed chan struct{}

func produce() int {
	num := rand.Intn(100)
	return num
}

func consume(num int) {
	fmt.Printf("[INFO] Consuming %d...\n", num)
}

func producer(ch chan<- int, wg *sync.WaitGroup, mutex chan struct{}) {
	defer wg.Done()
	for {
		select {
		case <-closed:
			fmt.Printf("[DEBUG] Received signal to stop producing. Returning...\n")
			return

		default:
			num := produce()
			ch <- num

			<-mutex
			produced++
			mutex <- struct{}{}

			fmt.Printf("[INFO] Produced %d!\n", num)
		}
	}
}

func consumer(ch <-chan int, wg *sync.WaitGroup, mutex chan struct{}) {
	defer wg.Done()
	for {
		select {
		case num := <-ch:
			consume(num)

			<-mutex
			consumed++
			mutex <- struct{}{}

		case <-time.After(time.Millisecond * CONSUMERS_TIME_LIMIT_MS):
			fmt.Printf("[DEBUG] No item has arrived in time. Returning...\n")
			return
		}
	}
}

func main() {
	ch := make(chan int, K)
	closed = make(chan struct{}, P)

	producerMutex := make(chan struct{}, 1)
	producerMutex <- struct{}{}

	consumerMutex := make(chan struct{}, 1)
	consumerMutex <- struct{}{}

	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup

	for i := 0; i < P; i++ {
		producerWg.Add(1)
		go producer(ch, &producerWg, producerMutex)
	}

	for i := 0; i < C; i++ {
		consumerWg.Add(1)
		go consumer(ch, &consumerWg, consumerMutex)
	}

	time.Sleep(time.Second * PRODUCERS_TIME_LIMIT_S)

	for i := 0; i < P; i++ {
		closed <- struct{}{}
	}

	producerWg.Wait()
	consumerWg.Wait()

	fmt.Print("\n=================================\n\n")
	fmt.Printf("[INFO] Produced total: %d\n", produced)
	fmt.Printf("[INFO] Consumed total: %d\n", consumed)
	fmt.Printf("[INFO] Are total produced and consumed equal? %t\n", produced == consumed)
}
