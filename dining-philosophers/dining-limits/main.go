package main

import (
	"fmt"
	"sync"
	"time"
)

const N = 5         // philosophers/forks
const R = 100       // iterations
const LIMIT = N - 1 // number of philosophers dining at a time

var forks [N]chan struct{}
var limit = make(chan struct{}, LIMIT)

func think(i int) {
	fmt.Printf("Hmmm... Philosopher %d is thinking...\n", i)
	time.Sleep(time.Second * 3)
}

func eat(i int) {
	fmt.Printf("Yummy! Philosopher %d is eating...\n", i)
	time.Sleep(time.Second * 3)
}

func takeForks(i int) {
	<-limit
	<-forks[i]
	<-forks[(i+1)%N]
}

func releaseForks(i int) {
	limit <- struct{}{}
	forks[i] <- struct{}{}
	forks[(i+1)%N] <- struct{}{}
}

/*
For non-buffered channels, operations need to be synchronized:
at the same time a channel is requesting resources, one must be offering.
This makes sure every fork will be correctly taken and released by each philosopher
*/
func fork(i int) {
	for {
		forks[i] <- struct{}{}
		<-forks[i]
	}
}

func philosopher(i int, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := 0; j < R; j++ {
		think(i)
		takeForks(i)
		eat(i)
		releaseForks(i)
		fmt.Printf("[INFO] Philosopher %d finished eating! Meals eaten: %d/%d\n", i, j+1, R)
	}
	fmt.Printf("[INFO] Philosopher %d has eaten all of his %d meals!\n", i, R)
}

func main() {
	var wg sync.WaitGroup

	for i := 0; i < LIMIT; i++ {
		limit <- struct{}{}
	}

	for i := 0; i < N; i++ {
		forks[i] = make(chan struct{})
		go fork(i)
	}

	for i := 0; i < N; i++ {
		wg.Add(1)
		go philosopher(i, &wg)
	}

	wg.Wait()
	fmt.Print("\n[INFO] Done! All philosophers have eaten all of their meals successfully\n")
}
