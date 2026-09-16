package queue

import "time"

type JobStatus string

const (
	JobStatusPending    JobStatus = "pending"
	JobStatusInProgress JobStatus = "in_progress"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
)

type Job struct {
	ID        string
	Type      string
	Payload   any
	Status    JobStatus
	Attempts  int
	CreatedAt time.Time
}

func NewJob(id string, jobType string, payload any) *Job {
	return &Job{
		ID:        id,
		Type:      jobType,
		Payload:   payload,
		Status:    JobStatusPending,
		Attempts:  0,
		CreatedAt: time.Now(),
	}
}

func (q *Queue) CreateJob(id string, jobType string, payload any) *Job {
	job := NewJob(id, jobType, payload)

	q.Jobs <- job

	return job
}

func (q *Queue) GetJob() *Job {
	select {
	case job := <-q.Jobs:
		return job
	default:
		return nil
	}
}

