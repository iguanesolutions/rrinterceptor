package influxrp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"time"

	influxcliv2 "github.com/influxdata/influxdb/client/v2"
)

const (
	showRP = "show retention policies;"
)

// GetRetentionPolicies returns the retention policies of the provided database at addr using user & password as auth
func GetRetentionPolicies(ctx context.Context, url *url.URL, database, user, password string) (rps RetentionPolicies, err error) {
	// Spawn influx client
	infcli, err := influxcliv2.NewHTTPClient(influxcliv2.HTTPConfig{
		Addr:      url.String(),
		Username:  user,
		Password:  password,
		UserAgent: "Iguane Solutions Sismology RRInterceptor",
	})
	if err != nil {
		err = fmt.Errorf("can't create influxdb client: %v", err)
		return
	}
	defer infcli.Close()
	// Execute it
	influxresp, err := infcli.QueryCtx(ctx, influxcliv2.NewQuery(showRP, database, "ms"))
	if err != nil {
		err = fmt.Errorf("can not execute '%s' on '%s': %v", showRP, database, err)
		return
	}
	if influxresp.Error() != nil {
		err = fmt.Errorf("'%s' returned an error: %v", showRP, influxresp.Error())
		return
	}
	// Extract rps
	return extractRP(influxresp.Results)
}

func extractRP(results []influxcliv2.Result) (rps RetentionPolicies, err error) {
	// Extract
	var (
		tmpName, tmpStr     string
		tmpDur, tmpShardDur time.Duration
		tmpReplicaRaw       json.Number
		tmpReplica          int64
		tmpDefaultVal, ok   bool
	)
	for resultIndex, result := range results {
		for serieIndex, serie := range result.Series {
			// Get column index
			nameIndex := -1
			durationIndex := -1
			sgDurationIndex := -1
			replicaIndex := -1
			defaultIndex := -1
			for index, name := range serie.Columns {
				switch name {
				case "name":
					nameIndex = index
				case "duration":
					durationIndex = index
				case "shardGroupDuration":
					sgDurationIndex = index
				case "replicaN":
					replicaIndex = index
				case "default":
					defaultIndex = index
				}
			}
			if nameIndex == -1 {
				err = fmt.Errorf("result #%d: serie #%d: nameIndex not found", resultIndex, serieIndex)
				return
			}
			if durationIndex == -1 {
				err = fmt.Errorf("result #%d: serie #%d: durationIndex not found", resultIndex, serieIndex)
				return
			}
			if sgDurationIndex == -1 {
				err = fmt.Errorf("result #%d: serie #%d: sgDurationIndex not found", resultIndex, serieIndex)
				return
			}
			if replicaIndex == -1 {
				err = fmt.Errorf("result #%d: serie #%d: replicaIndex not found", resultIndex, serieIndex)
				return
			}
			if defaultIndex == -1 {
				err = fmt.Errorf("result #%d: serie #%d: defaultIndex not found", resultIndex, serieIndex)
				return
			}
			// Parse RPs
			if rps == nil {
				rps = make(RetentionPolicies, len(serie.Values))
			}
			for rpIndex, rp := range serie.Values {
				// Name
				if tmpName, ok = rp[nameIndex].(string); !ok {
					err = fmt.Errorf("result #%d: serie #%d: rp #%d: can't cast '%v' as expected string as name",
						resultIndex, serieIndex, rpIndex, rp[nameIndex])
					return
				}
				// Duration
				if tmpStr, ok = rp[durationIndex].(string); !ok {
					err = fmt.Errorf("result #%d: serie #%d: rp #%d: can't cast '%v' as expected string as duration",
						resultIndex, serieIndex, rpIndex, rp[durationIndex])
					return
				}
				if tmpDur, err = time.ParseDuration(tmpStr); err != nil {
					err = fmt.Errorf("result #%d: serie #%d: rp #%d: can't parse '%s' as duration for duration: %v",
						resultIndex, serieIndex, rpIndex, tmpStr, err)
					return
				}
				// ShardGroupDuration
				if tmpStr, ok = rp[sgDurationIndex].(string); !ok {
					err = fmt.Errorf("result #%d: serie #%d: rp #%d: can't cast '%v' as expected string as shardGroupDuration",
						resultIndex, serieIndex, rpIndex, rp[sgDurationIndex])
					return
				}
				if tmpShardDur, err = time.ParseDuration(tmpStr); err != nil {
					err = fmt.Errorf("result #%d: serie #%d: rp #%d: can't parse '%s' as duration for shardGroupDuration: %v",
						resultIndex, serieIndex, rpIndex, tmpStr, err)
					return
				}
				// Replica
				if tmpReplicaRaw, ok = rp[replicaIndex].(json.Number); !ok {
					err = fmt.Errorf("result #%d: serie #%d: rp #%d: can't cast '%v' as expected string as replica",
						resultIndex, serieIndex, rpIndex, reflect.TypeOf(rp[replicaIndex]))
					return
				}
				if tmpReplica, err = tmpReplicaRaw.Int64(); err != nil {
					err = fmt.Errorf("result #%d: serie #%d: rp #%d: can't parse '%s' as int for replica: %v",
						resultIndex, serieIndex, rpIndex, tmpStr, err)
					return
				}
				// Default
				if tmpDefaultVal, ok = rp[defaultIndex].(bool); !ok {
					err = fmt.Errorf("result #%d: serie #%d: rp #%d: can't cast '%v' as expected string as replica",
						resultIndex, serieIndex, rpIndex, rp[defaultIndex])
					return
				}
				// Add RP
				rps[tmpName] = RetentionPolicy{
					Duration:           tmpDur,
					ShardGroupDuration: tmpShardDur,
					ReplicaN:           tmpReplica,
					Default:            tmpDefaultVal,
				}
			}
		}
	}
	return
}
