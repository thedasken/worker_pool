package main

import (
	"fmt"
)

type Result struct {
	JobID    int
	WorkerID int
	Value    int
}

var numbers = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
var workers = 3

func main() {
	// channels
	jobs := make(chan int)
	results := make(chan Result)

	// start workers
	for i := 1; i <= workers; i++ {
		go worker(i, jobs, results)
	}

	// send jobs
	go func() {
		defer close(jobs)
		for _, v := range numbers {
			jobs <- v
		}
	}()

	// read results
	for range numbers {
		res := <-results
		fmt.Printf("job %d traité par worker %d => résultat %d\n", res.JobID, res.WorkerID, res.Value)
	}
}

func worker(id int, jobs chan int, results chan Result) {
	for job := range jobs {
		fmt.Printf("worker %d commence job %d\n", id, job)
		result := job * 2
		res := Result{
			JobID:    job,
			WorkerID: id,
			Value:    result,
		}
		results <- res
		fmt.Printf("worker %d termine job %d\n", id, job)
	}
}
