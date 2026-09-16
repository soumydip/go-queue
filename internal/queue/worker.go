package queue

import (
	"fmt"
	"os"
)

// Worker processes jobs from the queue channel
func (q *Queue) Worker(workerID int) {
	for job := range q.Jobs {
		// 1. Status: In Progress
		job.Status = JobStatusInProgress
		fmt.Printf("[Worker %d] Started processing job ID: %s, Type: %s\n", workerID, job.ID, job.Type)

		if job.Type == "send_email" {
			data, ok := job.Payload.(map[string]any)

			if ok {
				to := data["to"].(string)
				subject := data["subject"].(string)
				message := data["message"].(string)

				fileName := to + ".txt"
				content := fmt.Sprintf("To: %s\nSubject: %s\nMessage: %s\n", to, subject, message)

				err := os.WriteFile(fileName, []byte(content), 0644)

				if err != nil {
					job.Status = JobStatusFailed
					fmt.Printf("[Worker %d] FAILED: Could not write to file '%s'. Error: %v\n", workerID, fileName, err)
					continue
				}

				fmt.Printf("[Worker %d] SUCCESS: File '%s' created successfully.\n", workerID, fileName)
			} else {
				job.Status = JobStatusFailed
				fmt.Printf("[Worker %d] ERROR: Invalid payload data.\n", workerID)
				continue
			}
		}

		job.Status = JobStatusCompleted
	}
}

// StartWorkers initializes and starts the worker goroutines
func (q *Queue) StartWorkers() {
	logFile, err := os.OpenFile("worker.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening log file: %v\n", err)
		return
	}
	defer logFile.Close()

	for i := 1; i <= q.Config.MaxWorkers; i++ {
		go q.Worker(i)

		logMessage := fmt.Sprintf("Worker %d started for queue: %s\n", i, q.Name)
		_, err = logFile.WriteString(logMessage)

		if err != nil {
			fmt.Printf("Error writing to log file: %v\n", err)
		}
	}
}
