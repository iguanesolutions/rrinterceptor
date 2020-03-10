package cacher

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"rrinterceptor/influxrp"
)

// GetRPs allows to get a cached
func (c *Controller) GetRPs(ctx context.Context, endpoint *url.URL, database, user, password string) (rps influxrp.RetentionPolicies, err error) {
	// First try to get cached rps
	cache := c.getOrCreate(database)
	defer cache.access.Unlock()
	cache.access.Lock()
	if cache.rps != nil {
		c.log.Debugf("[Cacher] rps found for '%s': using cache", database)
		rps = cache.rps
		return
	}
	// Else get them
	c.log.Debugf("[Cacher] no rps found for '%s': generating a new one", database)
	if rps, err = influxrp.GetRetentionPolicies(ctx, endpoint, database, user, password); err != nil {
		err = fmt.Errorf("previous rps did not exist and getting currents failed: %v", err)
		return
	}
	// And save it for others
	cache.rps = rps
	cache.created = time.Now()
	return
}

type cached struct {
	access  sync.Mutex
	rps     influxrp.RetentionPolicies
	created time.Time
}

func (c *Controller) getOrCreate(key string) (cache *cached) {
	var ok bool
	c.access.Lock()
	if cache, ok = c.cache[key]; !ok {
		cache = new(cached)
		c.cache[key] = cache
	}
	c.access.Unlock()
	return
}
