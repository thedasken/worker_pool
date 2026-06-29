package main

import (
	"fmt"
	"sync"
)

type Job struct {
	ID    int
	Value int
}

type Result struct {
	JobID    int
	WorkerID int
	Value    int
}

var numbers = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
var workers = 3

func main() {
	// channels
	jobs := make(chan Job)
	results := make(chan Result)

	var wg sync.WaitGroup

	// start workers
	for i := 1; i <= workers; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			worker(workerID, jobs, results)
		}(i)
	}

	// send jobs
	go func() {
		defer close(jobs)
		for i, v := range numbers {
			job := Job{
				ID:    i,
				Value: v,
			}
			jobs <- job
		}
	}()

	// read results
	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		fmt.Printf("job %d traité par worker %d => résultat %d\n", res.JobID, res.WorkerID, res.Value)
	}
}

func worker(id int, jobs chan Job, results chan Result) {
	for job := range jobs {
		fmt.Printf("worker %d commence job %d\n", id, job.ID)
		result := job.Value * 2
		fmt.Printf("worker %d termine job %d\n", id, job.ID)
		res := Result{
			JobID:    job.ID,
			WorkerID: id,
			Value:    result,
		}
		results <- res
	}
}
