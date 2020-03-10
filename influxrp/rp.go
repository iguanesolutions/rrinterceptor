package influxrp

import (
	"rrinterceptor/promutils"
	"time"
)

// RetentionPolicies is a collection of retention policies accesible by name
type RetentionPolicies map[string]RetentionPolicy

// GetClosest returns the name of smallest retention policy capable of handling StartTimestampMs
func (rp RetentionPolicies) GetClosest(StartTimestampMs int64) (name string) {
	// Parse timestamp to get date
	startDate := promutils.GetTimeFromTS(StartTimestampMs)
	// fmt.Println("Start Date:", startDate)
	// fmt.Println("Duration from Start Date:", time.Now().Sub(startDate))
	// Search for RP that contains this date and choose the closest
	now := time.Now()
	var delta, bestDelta time.Duration
	for retention, rpdata := range rp {
		// Special case: infinite is duration 0...
		if rpdata.Duration == 0 {
			// ...use it if nothing selected yet
			if name == "" {
				name = retention
			}
			continue
		}
		// Else compute delta
		delta = startDate.Sub(now.Add(rpdata.Duration * -1))
		// fmt.Println("Delta:", retention, delta)
		// Is this rp selectable ?
		if delta < 0 {
			continue // current RP can not have points for this TS
		}
		if bestDelta == 0 || delta < bestDelta {
			// either we don't have rp yet or this one is better
			name = retention
			bestDelta = delta
		}
	}
	return
}

// RetentionPolicy contains the metadata of a retention policy
type RetentionPolicy struct {
	Duration           time.Duration
	ShardGroupDuration time.Duration
	ReplicaN           int64
	Default            bool
}
