package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"rrinterceptor/cacher"

	"github.com/hekmon/hllogger"
	systemd "github.com/iguanesolutions/go-systemd"
)

var (
	cache      *cacher.Controller
	influxURL  *url.URL
	httpServer *http.Server
	httpProxy  *httputil.ReverseProxy
	log        *hllogger.HlLogger
	mainCtx    context.Context
	mainCancel context.CancelFunc
	mainLock   sync.Mutex
)

func main() {
	// cli flags
	var (
		bindAddr        = flag.String("bind-addr", ":9404", "The HTTP server bind address.")
		influxTarget    = flag.String("influx-url", "http://127.0.0.1:8086", "The influxdb target url.")
		checkFrequency  = flag.Int("check-frequency", 60, "The cache check frequency in minutes.")
		expirationLimit = flag.Int("expiration-limit", 1440, "The cache expiration limit.")
		logLevel        = flag.Int("log-level", 1, "Set the loglevel: Fatal(0) Error(1) Warning(2) Info(3) Debug(4).")
	)
	flag.Parse()

	var err error

	if influxURL, err = url.Parse(*influxTarget); err != nil {
		log.Fatalf(1, "[Main] Can't parse influxdb url: %v", err)
	}

	// Init logger
	var logLevelTyped hllogger.LogLevel
	if hllogger.LogLevel(*logLevel) > hllogger.Debug || hllogger.LogLevel(*logLevel) < hllogger.Fatal {
		fmt.Fprint(os.Stderr, "WARNING: log level is invalid, defaulting to Info(1)\n")
		logLevelTyped = hllogger.Info
	} else {
		logLevelTyped = hllogger.LogLevel(*logLevel)
	}
	log = hllogger.New(os.Stdout, &hllogger.Config{
		LogLevel:              logLevelTyped,
		SystemdJournaldCompat: systemd.IsNotifyEnabled(),
	})

	// Now that we have a logger, notify about sysd
	if systemd.IsNotifyEnabled() {
		log.Info("[Main] Systemd notifications supported and enabled")
	} else {
		log.Warning("[Main] Systemd notifications not supported")
	}

	// Create the app main context & lock
	mainCtx, mainCancel = context.WithCancel(context.Background())
	defer mainCancel() // make linter happy
	mainLock.Lock()

	// Create the cache & start the cleaner
	if cache, err = cacher.New(mainCtx, cacher.Config{
		CheckFrequency:  time.Duration(*checkFrequency) * time.Minute,
		ExpirationLimit: time.Duration(*expirationLimit) * time.Minute,
		Logger:          log,
	}); err != nil {
		log.Fatal(1, "[Main] Can't spawn cacher: is there a logger ?")
	}

	// Init the stats metrics
	if err = initMetrics(); err != nil {
		log.Fatalf(1, "[Main] Can't init stats metrics: %v", err)
	}

	// Init signal handler
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-term
		exit()
	}()

	// Launch the web server
	httpServer = &http.Server{
		Addr: *bindAddr,
	}
	http.HandleFunc("/smartread", wrapHandlerWithLogging(readHandler))
	http.Handle("/metrics", promHandler())
	log.Infof("[Main] Starting HTTP server on %s", *bindAddr)

	// Ready, start the server
	if err = systemd.NotifyReady(); err != nil {
		log.Errorf("[Main] Can't send systemd ready notification: %v", err)
	}
	if err = httpServer.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
		log.Errorf("[Main] HTTP Server: %v", err)
	}

	// Wait till the end...
	mainLock.Lock()
	log.Debug("[Main] Exiting main goroutine")
}

func exit() {
	log.Info("[Main] Exit triggered: gracefully stopping controllers")
	var err error
	if err = systemd.NotifyStopping(); err != nil {
		log.Errorf("[Main] Exit: can't send stopping notification to systemd: %v", err)
	}
	// First gracefully stop the http server
	timeout := 30 * time.Second
	log.Debugf("[Main] Stopping HTTP server gracefully (%v timeout)", timeout)
	stopContext, stopContextCancel := context.WithTimeout(mainCtx, timeout)
	defer stopContextCancel()
	if err = httpServer.Shutdown(stopContext); err != nil {
		log.Errorf("[Main] HTTP Server shutdown: %v", err)
	}
	// Then properly the other workers
	mainCancel()
	log.Debug("[Main] Stopping the cacher")
	cache.WaitFullStop()
	// Release the main gorouting to exit
	mainLock.Unlock()
}
