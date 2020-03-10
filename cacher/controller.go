package cacher

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hekmon/hllogger"
)

// Config allow to pass values to the contructor
type Config struct {
	CheckFrequency  time.Duration
	ExpirationLimit time.Duration
	Logger          *hllogger.HlLogger
}

// New returns an initialized and ready to use cache controller
func New(ctx context.Context, conf Config) (c *Controller, err error) {
	if conf.Logger == nil {
		err = errors.New("logger can't be nil")
		return
	}
	// Init controller
	c = &Controller{
		cache:   make(map[string]*cached, 1), // most usage will use 1 db
		log:     conf.Logger,
		ctx:     ctx,
		stopped: make(chan struct{}),
	}
	// Start workers
	c.workers.Add(1)
	go c.cleaner(conf.CheckFrequency, conf.ExpirationLimit)
	// Launch the stop watcher
	go c.stopWatcher()
	// All good
	return
}

// Controller allows to manage a cache instance
type Controller struct {
	// Cache
	access sync.Mutex
	cache  map[string]*cached
	// Sub Controllers
	log *hllogger.HlLogger
	// Workers
	ctx     context.Context
	workers sync.WaitGroup
	stopped chan struct{}
}

func (c *Controller) stopWatcher() {
	<-c.ctx.Done()
	c.log.Debugf("[Cacher] Stop signal received: waiting for workers to stop")
	c.workers.Wait()
	c.log.Debugf("[Cacher] All workers have stopped")
	close(c.stopped)
}

// WaitFullStop will block until all workers have ended
// folowing the cancellation of ctx
func (c *Controller) WaitFullStop() {
	<-c.stopped
}
