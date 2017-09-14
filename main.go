package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "net/url"
    "os"
    "path"
    "time"

    "github.com/DataDog/datadog-go/statsd"
    log "github.com/Sirupsen/logrus"
    "gopkg.in/alecthomas/kingpin.v2"
)

const (
    buflen    = 10240
    namespace = "logstash."
)

var (
    statsdAddr = kingpin.Flag("statsd", "Host:Port of Datadog Statsd agent").Required().String()
    lsURL      = kingpin.Flag("lsurl", "Logstash HTTP API endpoint").Default("http://127.0.0.1:9600").URL()
    interval   = kingpin.Flag("interval", "Gap between metric probes").Default("10s").Duration()
    debug      = kingpin.Flag("debug", "Enable debugging").Short('d').Bool()
)


type Events struct {
    Out float64 `json:"out"`
    In float64 `json:"in"`
}
type Capacity struct {
    Maxqsizeb float64 `json:"max_queue_size_in_bytes"`
    Qsizeb float64 `json:"queue_size_in_bytes"`
}
type Queue struct {
    Capacity `json:"capacity"`
}
type Pipeline struct {
    Events `json:"events"`
    Queue `json:"queue"`
}
type EventsOut struct {
    Pipeline `json:"pipeline"`
}

func GetEvents(URL url.URL) (*EventsOut, error) {
    var e EventsOut
    URL.Path = path.Join(URL.Path, "_node/stats/pipeline")
    s := URL.String()
    log.Debugf("Pulling node stats from: %s", s)
    resp, err := http.Get(s)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    json.Unmarshal(body, &e)
    if err != nil {
        return nil, err
    }
    return &e, nil
}

func main() {
    kingpin.Parse()
    if *debug {
        log.SetLevel(log.DebugLevel)
        log.Debugln("Debug logging enabled")
    } else {
        log.SetLevel(log.InfoLevel)
    }

    hostname, err := os.Hostname()
    if err != nil {
        log.Fatalf("Could not get hostname: %s", err)
    }
    tags := []string{fmt.Sprintf("nodename:%s", hostname)}
    // Statsd Client
    log.Infof("Starting a buffered statsd client at: %s", *statsdAddr)
    c, err := statsd.NewBuffered(*statsdAddr, buflen)
    if err != nil {
        log.Fatalf("Error starting statsd client: %s", err)
    }
    c.Namespace = namespace
    // Ticker
    var dtIn = interval.Seconds()
    var dtOut = interval.Seconds()
    ticker := time.Tick(*interval)
    var lastCountOut, currCountOut float64
    var lastCountIn, currCountIn float64
    var currQSizeB, currMaxQSizeB float64



    for {
        currVals, err := GetEvents(**lsURL)
        if err != nil {
            log.Errorf("Error getting event stats: %s", err)
            <-ticker
            continue
        }

        currCountOut = currVals.Pipeline.Events.Out
        currCountIn = currVals.Pipeline.Events.In
        currQSizeB = currVals.Pipeline.Queue.Capacity.Qsizeb
        currMaxQSizeB = currVals.Pipeline.Queue.Capacity.Maxqsizeb

        select {
        case _ = <-ticker:
            // Our only valid source of time is the tick, if the processing takes longer
            // than the tick then the value is invalid

            log.Warn("Tick happened before rate could be calculated, discarding value")
        default:

            if lastCountOut == 0 {
                lastCountOut = currCountOut
            } else {
                dyOut := currCountOut - lastCountOut
                rateOut := dyOut / dtOut
                if rateOut < 0 {
                    rateOut = 0
                }
                log.Debugf("Emitting rate Out: %.3f, with tags:%+v", rateOut, tags)
                c.Gauge("rateout", float64(rateOut), tags, 1)
                lastCountOut = currCountOut 
            }

            if lastCountIn == 0 {
                lastCountIn = currCountIn
            } else {
                dyIn := currCountIn - lastCountIn
                rateIn := dyIn / dtIn
                if rateIn < 0 {
                    rateIn = 0
                }
                log.Debugf("Emitting rate In: %.3f, with tags:%+v", rateIn, tags)
                c.Gauge("ratein", float64(rateIn), tags, 1)
                lastCountIn = currCountIn
            }
            log.Debugf("Emitting value: %.3f, with tags:%+v", currQSizeB, tags)
            c.Gauge("queue_size_in_bytes", float64(currQSizeB), tags, 1)
            log.Debugf("Emitting value: %.3f, with tags:%+v", currMaxQSizeB, tags)
            c.Gauge("max_queue_size_in_bytes", float64(currMaxQSizeB), tags, 1)
        }
        <-ticker
    }
}
