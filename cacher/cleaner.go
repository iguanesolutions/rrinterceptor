package cacher

import "time"

func (c *Controller) cleaner(frequency, controllerLifeLimit time.Duration) {
	defer c.workers.Done()
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.log.Debug("[Cacher] Cleaner: new ticker received, launching batch")
			c.cleanerBatch(controllerLifeLimit)
		case <-c.ctx.Done():
			c.log.Debug("[Cacher] Cleaner: cancel signal received")
			return
		}
	}
}

func (c *Controller) cleanerBatch(controllerLifeLimit time.Duration) {
	now := time.Now()
	c.access.Lock()
	for key, cache := range c.cache {
		cache.access.Lock()
		if now.Sub(cache.created) >= controllerLifeLimit {
			c.log.Infof("[Cacher] Cleaner: controller '%s' has reached is expiration date: deleting", key)
			delete(c.cache, key)
		}
		cache.access.Unlock()
	}
	c.access.Unlock()
}
