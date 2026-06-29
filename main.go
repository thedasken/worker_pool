package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Job struct {
	ID    int
	Value int
}

type Result struct {
	JobID    int
	WorkerID int
	Value    int
	Err      error
}

var numbers = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
var workers = 3

func main() {
	// buffered channels
	jobs := make(chan Job, len(numbers))
	results := make(chan Result, len(numbers))

	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// start workers
	for i := 1; i <= workers; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			worker(ctx, workerID, jobs, results)
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

	// close results when workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	successCount := 0
	errorCount := 0

	resultsByJobID := make([]Result, len(numbers))

	for res := range results {
		resultsByJobID[res.JobID] = res
	}

	for _, res := range resultsByJobID {
		if res.Err != nil {
			fmt.Printf("job %d échoué par worker %d => erreur %v\n", res.JobID, res.WorkerID, res.Err)
			errorCount++
			continue
		}

		fmt.Printf("job %d traité par worker %d => résultat %d\n", res.JobID, res.WorkerID, res.Value)
		successCount++
	}

	fmt.Println("\nRésumé:")
	fmt.Printf("succès: %d\n", successCount)
	fmt.Printf("erreurs: %d\n", errorCount)
	fmt.Printf("total: %d\n", successCount+errorCount)
}

func worker(ctx context.Context, id int, jobs <-chan Job, results chan<- Result) {
	for job := range jobs {
		fmt.Printf("worker %d commence job %d\n", id, job.ID)

		// call process to simulate an error scenario
		result, err := process(ctx, job)

		fmt.Printf("worker %d termine job %d\n", id, job.ID)

		res := Result{
			JobID:    job.ID,
			WorkerID: id,
			Value:    result,
			Err:      err,
		}
		results <- res
	}
}

func process(ctx context.Context, job Job) (int, error) {
	duree := time.Duration(job.Value) * 100 * time.Millisecond

	select {
	case <-time.After(duree):
		// finished working
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	if job.Value%4 == 0 {
		err := fmt.Errorf("valeur %d interdite: divisible par 4", job.Value)
		return 0, err
	}

	return job.Value * 2, nil
}
