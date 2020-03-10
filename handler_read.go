package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"rrinterceptor/influxrp"
	"rrinterceptor/promutils"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/hekmon/cunits"
	"github.com/miolini/datacounter"
	"github.com/prometheus/prometheus/prompb"
)

func readHandler(w *loggingResponseWriter, r *http.Request) {
	// Prepare
	start := time.Now()
	log.Debugf("[ReadHandler] Received '%s %s' from %s", r.Method, r.URL, r.RemoteAddr)
	var (
		err             error
		retentionPolicy string
		stepStart       time.Time
		streamSize      cunits.Bits
	)
	defer func() {
		if r.Context().Err() != nil {
			log.Infof("[ReadHandler] '%s %s' from '%s': client closed the connection after %v: aborting", r.Method, r.URL, r.RemoteAddr, time.Since(start))
		} else if err != nil {
			log.Infof("[ReadHandler] '%s %s' from '%s': answered '%d %s' in %v because of an error", r.Method, r.URL, r.RemoteAddr, w.statusCode, http.StatusText(w.statusCode), time.Since(start))
		} else {
			log.Infof("[ReadHandler] '%s %s' from '%s': proxified '%d %s' in %v (%s of data from %s)", r.Method, r.URL, r.RemoteAddr, w.statusCode, http.StatusText(w.statusCode), time.Since(start), streamSize, retentionPolicy)
		}
	}()
	stepStart = time.Now()
	// Extract prom request (first as it will close the original body)
	req, proceed := extractPromReq(w, r)
	if !proceed {
		return
	}
	go updateDriftStats(req)
	// Extract influxrp connection infos
	database, user, password, proceed := extractConInfo(w, r)
	if !proceed {
		return
	}
	log.Debugf("[ReadHandler] Extracting request data took %v", time.Since(stepStart))
	// Get the retention policies for this db
	stepStart = time.Now()
	retentionPolicies, err := cache.GetRPs(r.Context(), influxURL, database, user, password)
	if err != nil {
		if r.Context().Err() == nil {
			log.Errorf("[ReadHandler] can't get retention policies for '%s' db: %v", database, err)
			http.Error(w, fmt.Sprintf("can't get retention policies for '%s' db: %v", database, err), http.StatusInternalServerError)
		}
		return
	}
	// Get the best RP
	retentionPolicy, err = getBestRetentionPolicy(req.Queries, retentionPolicies)
	if err != nil {
		log.Errorf("[ReadHandler] can't select the best retention policy: %v", err)
		http.Error(w, fmt.Sprintf("can't select the best retention policy: %v", err), http.StatusBadRequest)
		return
	}
	log.Debugf("[ReadHandler] Getting retention policy took %v", time.Since(stepStart))
	// Debug the full request
	if log.IsDebugShown() {
		// request
		var buff strings.Builder
		buff.WriteString(" -> HEADERS\n")
		for key, values := range r.Header {
			for _, value := range values {
				buff.WriteString(fmt.Sprintf("%s: %s\n", key, value))
			}
		}
		buff.WriteString(" -> influxrp infos\n")
		buff.WriteString(fmt.Sprintf("Database: %s\n", database))
		buff.WriteString(fmt.Sprintf("User:     %s\n", user))
		if len(password) > 10 {
			buff.WriteString(fmt.Sprintf("Password: %s...%s\n", password[:3], password[len(password)-3:]))
		} else {
			buff.WriteString(fmt.Sprintf("Password: %s\n", password))
		}
		buff.WriteString(" -> REQUEST\n")
		buff.WriteString(promutils.BreakdownPromReadRequest(req))
		log.Debugf("[ReadHandler] Prometheus request breakdown: \n%s", buff.String())
		// rp
		buff.Reset()
		for rpName, rp := range retentionPolicies {
			buff.WriteString(fmt.Sprintf("\t%s: Duration(%v) ShardGroupDuration(%v) ReplicaN(%d) Default(%v)\n",
				rpName, rp.Duration, rp.ShardGroupDuration, rp.ReplicaN, rp.Default))
		}
		log.Debugf("[ReadHandler] %s: '%s' database: '%s' has been selected within the following rentention policies:\n%s", *influxURL, database, retentionPolicy, buff.String())
	}
	httpProxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = influxURL.Scheme
			req.URL.Host = influxURL.Host
			req.URL.Path = "/api/v1/prom/read"
			urlQuery := req.URL.Query()
			urlQuery.Set("rp", retentionPolicy)
			req.URL.RawQuery = urlQuery.Encode()
		},
		Transport: cleanhttp.DefaultTransport(),
	}
	wCounter := datacounter.NewResponseWriterCounter(w)
	httpProxy.ServeHTTP(wCounter, r)
	streamSize = cunits.Bits(wCounter.Count()) * cunits.Byte
}

func extractConInfo(w *loggingResponseWriter, r *http.Request) (database, user, password string, proceed bool) {
	if values, found := r.URL.Query()["db"]; found && len(values) != 0 {
		database = values[0]
	} else {
		if r.Context().Err() == nil {
			log.Errorf("[ReadHandler] no database found")
			http.Error(w, "can't extract database from URI parameters", http.StatusBadRequest)
		}
		return
	}
	if user, password, proceed = r.BasicAuth(); !proceed && r.Context().Err() == nil {
		log.Errorf("[ReadHandler] can't extract auth basic")
		http.Error(w, fmt.Sprintf("can't extract auth from header"), http.StatusBadRequest)
	}
	return
}

func extractPromReq(w *loggingResponseWriter, r *http.Request) (req prompb.ReadRequest, proceed bool) {
	// Extract body
	rawBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		if r.Context().Err() == nil {
			log.Errorf("[ReadHandler] can't extract body: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer r.Body.Close()
	// Restore body
	r.Body = ioutil.NopCloser(bytes.NewBuffer(rawBody))
	// Snappy decompress
	reqBuf, err := snappy.Decode(nil, rawBody)
	if err != nil {
		if r.Context().Err() == nil {
			log.Errorf("[ReadHandler] can't decode body as snappy: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	// Protobuff unmarshall
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		if r.Context().Err() == nil {
			log.Errorf("[ReadHandler] can't unmarshal snappy decompressed body as protobuff: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	// Done
	proceed = true
	return
}

func getBestRetentionPolicy(queries []*prompb.Query, rps influxrp.RetentionPolicies) (rp string, err error) {
	if len(queries) == 0 {
		err = errors.New("there must be at least one query")
		return
	}
	if len(rps) == 0 {
		err = errors.New("there must be at least one retention policy")
		return
	}
	var oldestStart int64
	for index, query := range queries {
		if index == 0 || query.StartTimestampMs < oldestStart {
			oldestStart = query.StartTimestampMs
		}
	}
	rp = rps.GetClosest(oldestStart)
	if rp == "" {
		err = fmt.Errorf("can't get a valid retention policy for query starting at %dms in %d retention policies",
			oldestStart, len(rps))
	}
	return
}
