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

var bufferSum = 0
var bufferSamples = 0

func produce() int {
	num := rand.Intn(100)
	return num
}

func consume(num int) {
	fmt.Printf("[INFO] Consuming %d...\n", num)
}

func producer(ch chan<- int, wg *sync.WaitGroup, mutex chan struct{}, done <-chan struct{}) {
	defer wg.Done()
	for {
		num := produce()

		select {
		case ch <- num:
			<-mutex
			produced++
			bufferSum += len(ch)
			bufferSamples++
			mutex <- struct{}{}

			fmt.Printf("[INFO] Produced %d!\n", num)

		case <-done:
			fmt.Printf("[DEBUG] Received signal to stop producing. Returning...\n")
			return
		}
	}
}

func consumer(ch <-chan int, wg *sync.WaitGroup, mutex chan struct{}, localTotal chan<- int) {
	defer wg.Done()
	defer close(localTotal)
	localConsumed := 0

	for {
		select {
		case num, ok := <-ch:
			if !ok {
				fmt.Printf("[DEBUG] Buffer has been closed. Returning...\n")
				localTotal <- localConsumed
				return
			}

			consume(num)
			localConsumed++

			<-mutex
			consumed++
			bufferSum += len(ch)
			bufferSamples++
			mutex <- struct{}{}

		case <-time.After(time.Millisecond * CONSUMERS_TIME_LIMIT_MS):
			fmt.Printf("[DEBUG] No item has arrived in time. Doing something else...\n")
			return
		}
	}
}

func main() {
	ch := make(chan int, K)
	done := make(chan struct{})

	producerMutex := make(chan struct{}, 1)
	producerMutex <- struct{}{}

	consumerMutex := make(chan struct{}, 1)
	consumerMutex <- struct{}{}

	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup

	consumedTotals := make([]chan int, C)
	for i := 0; i < C; i++ {
		consumedTotals[i] = make(chan int, 1)
	}

	startTime := time.Now()

	for i := 0; i < P; i++ {
		producerWg.Add(1)
		go producer(ch, &producerWg, producerMutex, done)
	}

	for i := 0; i < C; i++ {
		consumerWg.Add(1)
		go consumer(ch, &consumerWg, consumerMutex, consumedTotals[i])
	}

	time.Sleep(time.Second * PRODUCERS_TIME_LIMIT_S)

	close(done)
	producerWg.Wait()

	close(ch)
	consumerWg.Wait()

	totalDuration := time.Since(startTime)

	avgOccupancy := 0.0
	if bufferSamples > 0 {
		avgOccupancy = float64(bufferSum) / float64(bufferSamples)
	}

	fmt.Print("\n=================================\n\n")
	fmt.Printf("[INFO] Total duration: %v\n", totalDuration)
	fmt.Printf("[INFO] Total throughput: %.2f items/second\n", float64(consumed)/totalDuration.Seconds())
	fmt.Printf("[INFO] Average buffer occupancy: %.2f\n", avgOccupancy)

	fmt.Printf("[INFO] Produced total: %d\n", produced)
	fmt.Printf("[INFO] Consumed total: %d\n", consumed)
	fmt.Printf("[INFO] Are total produced and consumed equal? %t\n", produced == consumed)

	fmt.Print("\n=================================\n\n")
	for i := 0; i < C; i++ {
		fmt.Printf("[INFO] Consumer %d - Total consumed: %d\n", i, <-consumedTotals[i])
	}
}
