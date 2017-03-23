package main

import (
	"sync"
)

type Task interface {
	Execute() error
}

type Pool struct {
	mu     sync.Mutex
	Size   int
	tasks  chan Task
	errors chan error
	kill   chan struct{}
	wg     sync.WaitGroup
}

func NewPool(size int) *Pool {
	pool := &Pool{
		errors: make(chan error, 10),
		kill:   make(chan struct{}),
		tasks:  make(chan Task, 128),
	}
	pool.Resize(size)
	return pool
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			err := task.Execute()
			if err != nil {
				p.errors <- err
			}
		case <-p.kill:
			return
		}
	}
}

func (p *Pool) Resize(size int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.Size < size {
		p.Size++
		p.wg.Add(1)
		go p.worker()
	}
	for p.Size > size {
		p.Size--
		p.kill <- struct{}{}
	}
}

func (p *Pool) Exec(task Task) {
	p.tasks <- task
}

func (p *Pool) Close() {
	close(p.tasks)
}

func (p *Pool) Wait() {
	p.wg.Wait()
}
