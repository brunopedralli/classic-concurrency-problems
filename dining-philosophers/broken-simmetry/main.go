package main

import (
	"fmt"
	"sync"
	"time"
)

const N = 5   // philosophers/forks
const R = 100 // iterations

var forks [N]chan struct{}
var waitTimes [N]chan time.Duration
var meals [N]chan int

func think(i int) {
	fmt.Printf("Hmmm... Philosopher %d is thinking...\n", i)
	time.Sleep(time.Second * 3)
}

func eat(i int) {
	fmt.Printf("Yummy! Philosopher %d is eating...\n", i)
	time.Sleep(time.Second * 3)
}

func takeLeftForkFirst(i int) {
	<-forks[i]
	<-forks[(i+1)%N]
}

func takeRightForkFirst(i int) {
	<-forks[(i+1)%N]
	<-forks[i]
}

func releaseForks(i int) {
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
	defer close(waitTimes[i])
	defer close(meals[i])

	for j := 0; j < R; j++ {
		think(i)

		start := time.Now()
		if i%2 == 0 {
			takeLeftForkFirst(i)
		} else {
			takeRightForkFirst(i)
		}
		waitTime := time.Since(start)

		waitTimes[i] <- waitTime
		meals[i] <- 1

		eat(i)
		releaseForks(i)
		fmt.Printf("[INFO] Philosopher %d finished eating! Meals eaten: %d/%d\n", i, j+1, R)
	}
	fmt.Printf("[INFO] Philosopher %d has eaten all of his %d meals!\n", i, R)
}

func main() {
	var wg sync.WaitGroup

	for i := 0; i < N; i++ {
		forks[i] = make(chan struct{})
		waitTimes[i] = make(chan time.Duration, R)
		meals[i] = make(chan int, R)
		go fork(i)
	}

	for i := 0; i < N; i++ {
		wg.Add(1)
		go philosopher(i, &wg)
	}

	wg.Wait()
	fmt.Print("\n[INFO] Done! All philosophers have eaten all of their meals successfully\n")

	for i := 0; i < N; i++ {
		totalMeals := 0
		var totalWait time.Duration

		for meal := range meals[i] {
			totalMeals += meal
		}

		for wait := range waitTimes[i] {
			totalWait += wait
		}

		avgWait := time.Duration(0)
		if totalMeals > 0 {
			avgWait = totalWait / time.Duration(totalMeals)
		}

		fmt.Printf("Philosopher %d - Total meals: %d - Average wait for forks: %v\n", i, totalMeals, avgWait)
	}
}
