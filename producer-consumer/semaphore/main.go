package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const P = 2 // number of producers
const C = 2 // number of consumers
const K = 3 // shared buffer size

const PRODUCERS_TIME_LIMIT_S = 5
const CONSUMERS_TIME_LIMIT_MS = 100

var produced = 0
var consumed = 0

func produce() int {
	num := rand.Intn(100)
	return num
}

func consume(num int) {
	fmt.Printf("[INFO] Consuming %d...\n", num)
}

func producer(notFull <-chan struct{}, notEmpty chan<- struct{}, buffer chan<- int, wg *sync.WaitGroup, mutex chan struct{}, done <-chan struct{}) {
	defer wg.Done()
	for {
		num := produce()

		select {
		case <-notFull:
			buffer <- num
			notEmpty <- struct{}{}

			<-mutex
			produced++
			mutex <- struct{}{}

			fmt.Printf("[INFO] Produced %d!\n", num)

		case <-done:
			fmt.Printf("[DEBUG] Received signal to stop producing. Returning...\n")
			return
		}
	}
}

func consumer(notFull chan<- struct{}, notEmpty <-chan struct{}, buffer <-chan int, wg *sync.WaitGroup, mutex chan struct{}, done <-chan struct{}) {
	defer wg.Done()
	for {
		select {
		case <-notEmpty:
			num := <-buffer
			notFull <- struct{}{}
			consume(num)

			<-mutex
			consumed++
			mutex <- struct{}{}

		case <-done:
			for num := range buffer {
				consume(num)

				<-mutex
				consumed++
				mutex <- struct{}{}
			}
			fmt.Printf("[DEBUG] Buffer has been drained. Returning...\n")
			return

		case <-time.After(time.Millisecond * CONSUMERS_TIME_LIMIT_MS):
			fmt.Printf("[DEBUG] No item has arrived in time. Doing something else...\n")
		}
	}
}

func main() {
	buffer := make(chan int, K)        // finite queue
	notEmpty := make(chan struct{}, K) // initialized with 0
	notFull := make(chan struct{}, K)  // initialized with K
	done := make(chan struct{})

	producerMutex := make(chan struct{}, 1)
	producerMutex <- struct{}{}

	consumerMutex := make(chan struct{}, 1)
	consumerMutex <- struct{}{}

	for i := 0; i < K; i++ {
		notFull <- struct{}{}
	}

	var producerWg sync.WaitGroup
	for i := 0; i < P; i++ {
		producerWg.Add(1)
		go producer(notFull, notEmpty, buffer, &producerWg, producerMutex, done)
	}

	var consumerWg sync.WaitGroup
	for i := 0; i < C; i++ {
		consumerWg.Add(1)
		go consumer(notFull, notEmpty, buffer, &consumerWg, consumerMutex, done)
	}

	time.Sleep(time.Second * PRODUCERS_TIME_LIMIT_S)

	close(done)
	producerWg.Wait()

	close(buffer)
	consumerWg.Wait()

	fmt.Print("\n=================================\n\n")
	fmt.Printf("[INFO] Produced total: %d\n", produced)
	fmt.Printf("[INFO] Consumed total: %d\n", consumed)
	fmt.Printf("[INFO] Are total produced and consumed equal? %t\n", produced == consumed)
}
