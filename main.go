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
	Done     bool
}

type Processor func(context.Context, Job) (int, error)

var numbers = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
var workerCount = 3

func main() {
	jobList := buildJobs(numbers)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resultsByJobID := Run(ctx, jobList, workerCount, process)

	printResults(resultsByJobID)
}

func worker(ctx context.Context, id int, jobs <-chan Job, results chan<- Result, processor Processor) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("worker %d arrêté\n", id)
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			if ctx.Err() != nil {
				fmt.Printf("worker %d arrêté avant traitement du job %d\n", id, job.ID)
				return
			}

			fmt.Printf("worker %d commence job %d\n", id, job.ID)
			result, err := processor(ctx, job)
			fmt.Printf("worker %d termine job %d\n", id, job.ID)
			res := Result{
				JobID:    job.ID,
				WorkerID: id,
				Value:    result,
				Err:      err,
				Done:     true,
			}

			select {
			case results <- res:
			case <-ctx.Done():
				fmt.Printf("worker %d arrêté\n", id)
				return
			}
		}
	}
}

func process(ctx context.Context, job Job) (int, error) {
	duree := time.Duration(job.Value) * 100 * time.Millisecond

	select {
	case <-time.After(duree):
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	if job.Value%4 == 0 {
		err := fmt.Errorf("valeur %d interdite: divisible par 4", job.Value)
		return 0, err
	}

	return job.Value * 2, nil
}

func Run(ctx context.Context, jobList []Job, workerCount int, processor Processor) []Result {
	jobs := make(chan Job, len(jobList))
	results := make(chan Result, len(jobList))

	var wg sync.WaitGroup

	// start workers
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			worker(ctx, workerID, jobs, results, processor)
		}(i)
	}

	// send jobs
	go func() {
		defer close(jobs)
		for _, job := range jobList {
			select {
			case jobs <- job:
			case <-ctx.Done():
				return
			}
		}
	}()

	// close results when workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	resultsByJobID := make([]Result, len(jobList))

	for res := range results {
		resultsByJobID[res.JobID] = res
	}

	return resultsByJobID
}

func printResults(resultsByJobID []Result) {
	successCount := 0
	errorCount := 0
	skippedCount := 0

	for jobID, res := range resultsByJobID {
		if !res.Done {
			fmt.Printf("job %d sans résultat reçu\n", jobID)
			skippedCount++
			continue
		}

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
	fmt.Printf("sans résultat reçu: %d\n", skippedCount)
	fmt.Printf("total: %d\n", successCount+errorCount+skippedCount)
}

func buildJobs(numbers []int) []Job {
	jobs := make([]Job, 0, len(numbers))

	for i, number := range numbers {
		job := Job{
			ID:    i,
			Value: number,
		}
		jobs = append(jobs, job)
	}

	return jobs
}
