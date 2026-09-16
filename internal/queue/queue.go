package queue

import (
	"errors"
	"strings"
	"sync"
	"time"
)

type QueueConfig struct {
	RetryOnFailure bool
	RetryLimit     int
	MaxWorkers     int
	Timeout        time.Duration
}

type Queue struct {
	Name   string
	Config QueueConfig
	Jobs   chan *Job
}

type QueueManager struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

func NewQueueManager() *QueueManager {
	return &QueueManager{
		queues: make(map[string]*Queue),
	}
}

func (qm *QueueManager) CreateQueue(queue *Queue) error {
	if qm == nil {
		return errors.New("queue manager is nil")
	}

	if queue == nil {
		return errors.New("queue is nil")
	}

	if strings.TrimSpace(queue.Name) == "" {
		return errors.New("queue name is required")
	}

	if queue.Config.RetryLimit < 0 {
		return errors.New("retry limit cannot be negative")
	}

	if queue.Config.MaxWorkers <= 0 {
		return errors.New("max workers must be greater than 0")
	}

	if queue.Config.Timeout <= 0 {
		return errors.New("timeout must be greater than 0")
	}

	if queue.Jobs == nil {
		queue.Jobs = make(chan *Job, 100)
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()

	if _, exists := qm.queues[queue.Name]; exists {
		return errors.New("queue already exists")
	}

	qm.queues[queue.Name] = queue

	// start workers for the queue
	queue.StartWorkers()

	return nil
}

func (qm *QueueManager) IsQueueExists(queueName string) bool {
	if qm == nil {
		return false
	}

	qm.mu.RLock()
	defer qm.mu.RUnlock()

	_, exists := qm.queues[queueName]

	return exists
}

func (qm *QueueManager) GetQueue(queueName string) (*Queue, error) {
	if qm == nil {
		return nil, errors.New("queue manager is nil")
	}

	qm.mu.RLock()
	defer qm.mu.RUnlock()

	queue, exists := qm.queues[queueName]

	if !exists {
		return nil, errors.New("queue not found")
	}

	return queue, nil
}

func (qm *QueueManager) DeleteQueue(queueName string) error {
	if qm == nil {
		return errors.New("queue manager is nil")
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()

	if _, exists := qm.queues[queueName]; !exists {
		return errors.New("queue not found")
	}

	delete(qm.queues, queueName)

	return nil
}
