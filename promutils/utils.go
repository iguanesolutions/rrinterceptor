package promutils

import (
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

// GetEffectiveStart returns the best ms start timestamp of a query
func GetEffectiveStart(q *prompb.Query) (startms int64) {
	if q == nil {
		return 0
	}
	if q.Hints != nil && q.Hints.StartMs != 0 {
		return q.Hints.StartMs
	}
	return q.StartTimestampMs
}

// GetEffectiveEnd returns the best ms end timestamp of a query
func GetEffectiveEnd(q *prompb.Query) (startms int64) {
	if q == nil {
		return 0
	}
	if q.Hints != nil && q.Hints.EndMs != 0 {
		return q.Hints.EndMs
	}
	return q.EndTimestampMs
}

// IsSteppingUsable returns true if a stepping has been provided
func IsSteppingUsable(q *prompb.Query) (present bool) {
	if q == nil {
		return
	}
	if q.Hints == nil {
		return
	}
	if q.Hints.StepMs == 0 {
		return
	}
	return true
}

// GetTimeFromTS return a golang Time based on timestamp in milliseconds
func GetTimeFromTS(ms int64) time.Time {
	return time.Unix(ms/1000, (ms%1000)*1000000)
}

// BreakdownPromReadRequest returns a multi lines, human readable breakdown of a prometheus read request
func BreakdownPromReadRequest(req prompb.ReadRequest) (multilines string) {
	var buffer strings.Builder
	for index, query := range req.Queries {
		buffer.WriteString(fmt.Sprintf("Query #%d\n", index+1))
		buffer.WriteString(fmt.Sprintf("  StartTimestampMs: %d (%v)\n", query.StartTimestampMs, GetTimeFromTS(query.StartTimestampMs)))
		buffer.WriteString(fmt.Sprintf("  EndTimestampMs:   %d (%v)\n", query.EndTimestampMs, GetTimeFromTS(query.EndTimestampMs)))
		buffer.WriteString(fmt.Sprintf("  Matchers:\n"))
		for index, matcher := range query.GetMatchers() {
			buffer.WriteString(fmt.Sprintf("    Matcher #%d\n", index+1))
			buffer.WriteString(fmt.Sprintf("      Type:  %s\n", matcher.Type))
			buffer.WriteString(fmt.Sprintf("      Name:  %s\n", matcher.Name))
			buffer.WriteString(fmt.Sprintf("      Value: %s\n", matcher.Value))
		}
		if query.Hints != nil {
			buffer.WriteString(fmt.Sprintf("  Hints:\n"))
			buffer.WriteString(fmt.Sprintf("    StepMs:  %d\n", query.Hints.StepMs))
			buffer.WriteString(fmt.Sprintf("    Func:    %s\n", query.Hints.Func))
			buffer.WriteString(fmt.Sprintf("    StartMs: %d (%v)\n", query.Hints.StartMs, GetTimeFromTS(query.Hints.StartMs)))
			buffer.WriteString(fmt.Sprintf("    EndMs:   %d (%v)\n", query.Hints.EndMs, GetTimeFromTS(query.Hints.EndMs)))
		} else {
			buffer.WriteString("  Hints:  nil\n")
		}
	}
	return buffer.String()
}
