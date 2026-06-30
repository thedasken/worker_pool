package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunAllSuccess(t *testing.T) {
	jobList := []Job{
		{ID: 0, Value: 1},
		{ID: 1, Value: 2},
		{ID: 2, Value: 3},
	}

	processor := func(ctx context.Context, job Job) (int, error) {
		return job.Value * 10, nil
	}

	ctx := context.Background()

	results := Run(ctx, jobList, 2, processor)

	got := len(results)
	if got != len(jobList) {
		t.Fatalf("want %d results, got %d", len(jobList), got)
	}

	for i := range results {
		result := results[i]
		job := jobList[i]

		if !result.Done {
			t.Fatalf("want result.Done to be true, got %v", result.Done)
		}

		if result.Err != nil {
			t.Fatalf("want result.Err to be nil, got %v", result.Err)
		}

		if result.JobID != job.ID {
			t.Fatalf("want result.JobID %d, got %d", job.ID, result.JobID)
		}

		wantValue := job.Value * 10
		if result.Value != wantValue {
			t.Fatalf("want value %d, got %d", wantValue, result.Value)
		}

		if result.WorkerID < 1 || result.WorkerID > 2 {
			t.Fatalf("want worker ID between 1 and 2, got %d", result.WorkerID)
		}
	}
}

func TestRunWithProcessorError(t *testing.T) {
	jobList := []Job{
		{ID: 0, Value: 1},
		{ID: 1, Value: 2},
		{ID: 2, Value: 3},
	}

	expectedErr := errors.New("processor error")

	processor := func(ctx context.Context, job Job) (int, error) {
		if job.Value == 2 {
			return 0, expectedErr
		}

		return job.Value * 10, nil
	}

	ctx := context.Background()

	workerCount := 2
	results := Run(ctx, jobList, workerCount, processor)

	if len(results) != len(jobList) {
		t.Fatalf("want %d results, got %d", len(jobList), len(results))
	}

	result := results[1]
	assertError(t, result, 1, expectedErr, workerCount)

	result = results[0]
	assertSuccess(t, result, 0, 10, workerCount)

	result = results[2]
	assertSuccess(t, result, 2, 30, workerCount)
}

func assertSuccess(t *testing.T, result Result, wantJobID int, wantValue int, workerCount int) {
	t.Helper()

	if result.JobID != wantJobID {
		t.Fatalf("want jobID %d, got %d", wantJobID, result.JobID)
	}

	if !result.Done {
		t.Fatalf("want result.Done to be true, got %v", result.Done)
	}

	if result.Value != wantValue {
		t.Fatalf("want value %d, got %d", wantValue, result.Value)
	}

	if result.Err != nil {
		t.Fatalf("expect error %v, got %v", nil, result.Err)
	}

	if result.WorkerID < 1 || result.WorkerID > workerCount {
		t.Fatalf("want worker ID between 1 and %d, got %d", workerCount, result.WorkerID)
	}
}

func assertError(t *testing.T, result Result, wantJobID int, expectedErr error, workerCount int) {
	t.Helper()

	if result.JobID != wantJobID {
		t.Fatalf("want jobID %d, got %d", wantJobID, result.JobID)
	}

	if !result.Done {
		t.Fatalf("want result.Done to be true, got %v", result.Done)
	}

	if result.Value != 0 {
		t.Fatalf("want value %d, got %d", 0, result.Value)
	}

	if result.Err != expectedErr {
		t.Fatalf("expect error %v, got %v", expectedErr, result.Err)
	}

	if result.WorkerID < 1 || result.WorkerID > workerCount {
		t.Fatalf("want worker ID between 1 and %d, got %d", workerCount, result.WorkerID)
	}
}

func TestRunWithNoJobs(t *testing.T) {
	processor := func(ctx context.Context, job Job) (int, error) {
		t.Fatal("processor should not be called")
		return 0, nil
	}

	results := Run(context.Background(), nil, 3, processor)

	if len(results) != 0 {
		t.Fatalf("want 0 results, got %d", len(results))
	}
}

func TestRunWithOneWorker(t *testing.T) {
	jobList := []Job{
		{ID: 0, Value: 1},
		{ID: 1, Value: 2},
		{ID: 2, Value: 3},
	}

	processor := func(ctx context.Context, job Job) (int, error) {
		return job.Value * 10, nil
	}

	ctx := context.Background()

	results := Run(ctx, jobList, 1, processor)

	got := len(results)
	if got != len(jobList) {
		t.Fatalf("want %d results, got %d", len(jobList), got)
	}

	for i := range results {
		result := results[i]
		job := jobList[i]

		if !result.Done {
			t.Fatalf("want result.Done to be true, got %v", result.Done)
		}

		if result.Err != nil {
			t.Fatalf("want result.Err to be nil, got %v", result.Err)
		}

		if result.JobID != job.ID {
			t.Fatalf("want result.JobID %d, got %d", job.ID, result.JobID)
		}

		wantValue := job.Value * 10
		if result.Value != wantValue {
			t.Fatalf("want value %d, got %d", wantValue, result.Value)
		}

		if result.WorkerID != 1 {
			t.Fatalf("want worker ID 1, got %d", result.WorkerID)
		}
	}
}

func TestRunWithTimeout(t *testing.T) {
	jobList := []Job{
		{ID: 0, Value: 1},
		{ID: 1, Value: 2},
		{ID: 2, Value: 3},
		{ID: 3, Value: 4},
	}

	processor := func(ctx context.Context, job Job) (int, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			return job.Value * 10, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	results := Run(ctx, jobList, 2, processor)

	if len(results) != len(jobList) {
		t.Fatalf("want %d results, got %d", len(jobList), len(results))
	}

	hasInterruptedJob := false

	for _, result := range results {
		if !result.Done || result.Err != nil {
			hasInterruptedJob = true
			break
		}
	}

	if !hasInterruptedJob {
		t.Fatalf("want at least one interrupted job, got none")
	}
}
