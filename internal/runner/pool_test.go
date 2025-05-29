package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestJobAndPoolIntegration_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var completedJobs []string

	handler := func(ctx context.Context, job *Job) error {
		// Simulate work
		time.Sleep(10 * time.Millisecond)
		return nil
	}
	callback := func(ctx context.Context, job *Job) {
		mu.Lock()
		defer mu.Unlock()
		completedJobs = append(completedJobs, job.Name())
	}

	pool := NewPool(ctx, "test-pool",
		WithPoolWorkers(3),
	)

	// Submit jobs
	jobCount := 5
	for i := range jobCount {
		job := NewJob(
			"job-success-"+string(rune(i)),
			handler,
			WithJobCallback(callback),
		)
		err := pool.Submit(job)
		if err != nil {
			t.Fatalf("failed to submit job: %v", err)
		}
	}

	// Wait for all jobs to complete
	completed := 0
	for completed < jobCount {
		job, err := pool.Wait()
		if err != nil {
			t.Fatalf("error waiting for job: %v", err)
		}
		if !job.Tracker().IsSucceeded() {
			t.Errorf("job %s did not succeed, status: %s", job.Name(), job.Tracker().Status())
		}
		completed++
	}

	if len(completedJobs) != jobCount {
		t.Errorf("expected %d completed jobs, got %d", jobCount, len(completedJobs))
	}
}

func TestJobAndPoolIntegration_Failure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler := func(ctx context.Context, job *Job) error {
		return errors.New("fail job")
	}

	pool := NewPool(ctx, "test-pool-fail",
		WithPoolWorkers(2),
	)

	job := NewJob("job-fail", handler)
	err := pool.Submit(job)
	if err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	completedJob, err := pool.Wait()
	if err != nil {
		t.Fatalf("error waiting for job: %v", err)
	}
	if !completedJob.Tracker().IsFailed() {
		t.Errorf("expected job to fail, got status: %s", completedJob.Tracker().Status())
	}
}

func TestJobAndPool_TrackerProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	total := int64(100)
	handler := func(ctx context.Context, job *Job) error {
		job.Tracker().SetTotal(total)
		for i := int64(0); i < total; i += 10 {
			job.Tracker().IncCurrent(10)
			time.Sleep(1 * time.Millisecond)
		}
		return nil
	}

	pool := NewPool(ctx, "test-pool-progress",
		WithPoolWorkers(1),
	)

	job := NewJob("job-progress", handler)
	err := pool.Submit(job)
	if err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	completedJob, err := pool.Wait()
	if err != nil {
		t.Fatalf("error waiting for job: %v", err)
	}
	if !completedJob.Tracker().IsSucceeded() {
		t.Errorf("expected job to succeed, got status: %s", completedJob.Tracker().Status())
	}
	if completedJob.Tracker().Current() != total {
		t.Errorf("expected tracker current to be %d, got %d", total, completedJob.Tracker().Current())
	}
	if completedJob.Tracker().Percent() != 100 {
		t.Errorf("expected tracker percent to be 100, got %f", completedJob.Tracker().Percent())
	}
}

func TestJobAndPool_IntegrationWorkflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		mu                 sync.Mutex
		results            = make(map[string]int)
		completed          []string
		poolCallbackCalled bool
	)

	// Job 1: Produces a value
	job1Handler := func(ctx context.Context, job *Job) error {
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		results[job.Name()] = 42
		mu.Unlock()
		return nil
	}

	// Job 2: Consumes job1's value, produces another
	job2Handler := func(ctx context.Context, job *Job) error {
		mu.Lock()
		v, ok := results["job1"]
		mu.Unlock()
		if !ok {
			return errors.New("job1 result missing")
		}
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		results[job.Name()] = v + 1
		mu.Unlock()
		return nil
	}

	// Job 3: Consumes job2's value
	job3Handler := func(ctx context.Context, job *Job) error {
		mu.Lock()
		v, ok := results["job2"]
		mu.Unlock()
		if !ok {
			return errors.New("job2 result missing")
		}
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		results[job.Name()] = v * 2
		mu.Unlock()
		return nil
	}

	jobCallback := func(ctx context.Context, job *Job) {
		mu.Lock()
		completed = append(completed, job.Name())
		mu.Unlock()
	}

	poolCallback := func(ctx context.Context, pool *Pool) {
		poolCallbackCalled = true
	}

	pool := NewPool(ctx, "integration-pool",
		WithPoolWorkers(2),
		WithPoolCallback(poolCallback),
	)

	// Submit jobs in dependency order, but only start next after previous completes
	job1 := NewJob("job1", job1Handler, WithJobCallback(jobCallback))
	err := pool.Submit(job1)
	if err != nil {
		t.Fatalf("failed to submit job1: %v", err)
	}
	j1, err := pool.Wait()
	if err != nil {
		t.Fatalf("waiting for job1: %v", err)
	}
	if !j1.Tracker().IsSucceeded() {
		t.Errorf("job1 did not succeed")
	}

	job2 := NewJob("job2", job2Handler, WithJobCallback(jobCallback))
	err = pool.Submit(job2)
	if err != nil {
		t.Fatalf("failed to submit job2: %v", err)
	}
	j2, err := pool.Wait()
	if err != nil {
		t.Fatalf("waiting for job2: %v", err)
	}
	if !j2.Tracker().IsSucceeded() {
		t.Errorf("job2 did not succeed")
	}

	job3 := NewJob("job3", job3Handler, WithJobCallback(jobCallback))
	err = pool.Submit(job3)
	if err != nil {
		t.Fatalf("failed to submit job3: %v", err)
	}
	j3, err := pool.Wait()
	if err != nil {
		t.Fatalf("waiting for job3: %v", err)
	}
	if !j3.Tracker().IsSucceeded() {
		t.Errorf("job3 did not succeed")
	}

	// Signal no more jobs will be submitted by cancelling the context
	cancel()

	// Wait for pool callback to be called (up to 1s)
	for range 100 {
		if poolCallbackCalled {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Check results
	mu.Lock()
	if results["job1"] != 42 {
		t.Errorf("job1 result = %d, want 42", results["job1"])
	}
	if results["job2"] != 43 {
		t.Errorf("job2 result = %d, want 43", results["job2"])
	}
	if results["job3"] != 86 {
		t.Errorf("job3 result = %d, want 86", results["job3"])
	}
	if len(completed) != 3 {
		t.Errorf("expected 3 completed jobs, got %d", len(completed))
	}
	mu.Unlock()

	if !poolCallbackCalled {
		t.Errorf("pool callback was not called")
	}

	// Check pool tracker
	if pool.Tracker().Current() != 3 {
		t.Errorf("pool tracker current = %d, want 3", pool.Tracker().Current())
	}
	if pool.Tracker().Total() != 3 {
		t.Errorf("pool tracker total = %d, want 3", pool.Tracker().Total())
	}
}
