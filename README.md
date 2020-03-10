# Remote Read Interceptor

Remote Read Interceptor is a reverse proxy to be placed between a Prometheus and a "remote-read" InfluxDB. It allows to dynamically change the influxdb database RP used to gather points based on the Prometheus remote read request time range.

## Install

Remote Read Interceptor binaries have no dependencies. You can get the latest under release section.

## Build

To build from source you will need **[Git](https://git-scm.com/downloads)** and **[Go](https://golang.org/doc/install)**.

- Run `go get github.com/iguanesolutions/rrinterceptor`

Remote Read Interceptor will be installed to your `$GOPATH/bin` folder.

## Flags

Remote Read Interceptor has the following command-line flags:

* `-bind-addr` - the HTTP server bind address (default: ':9404').
* `-influx-url` - the influxdb target url (default: 'http://127.0.0.1:8086').
* `-check-frequency` - the cache check frequency in minutes (default: 60).
* `-expiration-limit` - the cache expiration limit (default: 1440).
* `-log-level` - set the loglevel: Fatal(0) Error(1) Warning(2) Info(3) Debug(4) (default: '1').

## Prometheus setup

Prometheus must be configured with [remote_read](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read)
in order to read data thought Remote Read Interceptor.
Add the following lines to Prometheus configuration file (it is usually located at `/etc/prometheus/prometheus.yml`):

```yaml
remote_read:
  - url: 'http://127.0.0.1:9404/smartread?db=influx'
```

The [remote_write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write)  writes data directly to influxdb.
Add the following lines to Prometheus configuration file:

```yaml
remote_write:
  - url: 'http://127.0.0.1:8086/api/v1/prom/write?db=influx'
```
