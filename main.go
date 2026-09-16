package main

import (
	"fmt"
	"goqueue/internal/queue"
	"time"
)

func main() {
	qm := queue.NewQueueManager()

	emailQueue := &queue.Queue{
		Name: "email",
		Config: queue.QueueConfig{
			RetryOnFailure: true,
			RetryLimit:     2,
			MaxWorkers:     5,
			Timeout:        30 * time.Second,
		},
	}

	// Create queue
	// Workers automatically start here
	err := qm.CreateQueue(emailQueue)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Queue exists:", qm.IsQueueExists("email"))

	// Jobs to create
	jobs := []struct {
		id      string
		jobType string
		payload map[string]any
	}{
		{
			id:      "job-1",
			jobType: "send_email",
			payload: map[string]any{
				"to":      "user1@example.com",
				"subject": "Welcome",
				"message": "Welcome to our platform",
			},
		},
		{
			id:      "job-2",
			jobType: "send_email",
			payload: map[string]any{
				"to":      "user2@example.com",
				"subject": "Verify Email",
				"message": "Please verify your email",
			},
		},
		{
			id:      "job-3",
			jobType: "send_email",
			payload: map[string]any{
				"to":      "user3@example.com",
				"subject": "Password Reset",
				"message": "Reset your password",
			},
		},
		{
			id:      "job-4",
			jobType: "send_email",
			payload: map[string]any{
				"to":      "user4@example.com",
				"subject": "Notification",
				"message": "You have a new notification",
			},
		},
		{
			id:      "job-5",
			jobType: "send_email",
			payload: map[string]any{
				"to":      "user5@example.com",
				"subject": "Order Update",
				"message": "Your order has been updated",
			},
		},
	}

	// Create multiple jobs
	for _, data := range jobs {
		job := emailQueue.CreateJob(
			data.id,
			data.jobType,
			data.payload,
		)

		fmt.Println("\nJob created:")
		fmt.Println("ID:", job.ID)
		fmt.Println("Type:", job.Type)
		fmt.Println("Status:", job.Status)
		fmt.Println("Attempts:", job.Attempts)
		fmt.Println("CreatedAt:", job.CreatedAt)
	}

	// Give workers time to process jobs
	time.Sleep(2 * time.Second)
}
