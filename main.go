package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type headerFlags []string

func (h *headerFlags) String() string {
	return fmt.Sprintf("%v", *h)
}

func (h *headerFlags) Set(s string) error {
	*h = append(*h, strings.TrimSpace(s))
	return nil
}

type core struct {
	client  *http.Client
	method  string
	url     string
	headers http.Header

	errCount     int32
	successCount int32

	outChan chan string
	outDone chan struct{}
	out     io.Writer

	wg       sync.WaitGroup
	routines int32
	done     chan struct{}
}

type rate struct {
	rate      int
	increment int

	nextTick time.Time
	nextInc  time.Time
}

func newRate(r, inc int) *rate {
	return &rate{
		rate:      r,
		increment: inc,

		nextInc: time.Now().Add(time.Second),
	}
}

func (r *rate) SetNext() {
	freq := time.Duration(1.0 / float64(r.rate) * float64(time.Second))
	r.nextTick = time.Now().Add(freq)
}

func (r *rate) Pause() {
	// Now we need to wait for the next interval
	waitTime := time.Now().Sub(r.nextTick)
	if waitTime < time.Millisecond {
		for time.Now().Before(r.nextTick) {
		}
	} else {
		time.Sleep(waitTime)
	}

	// Increment the rate if needed
	if r.increment > 0 {
		for time.Now().After(r.nextInc) {
			r.rate += r.increment
			r.nextInc = r.nextInc.Add(time.Second)
		}
	}
}

var _headersValue headerFlags
var (
	_rate        = flag.Int("rate", 1, "RPS for the test")
	_increment   = flag.Int("increment", 0, "RPS that the rate increments linearly over time")
	_test        = flag.Bool("test", false, "Issues a single test request and prints the output")
	_time        = flag.Duration("time", 0, "How long the test should run")
	_method      = flag.String("method", http.MethodGet, "The method to use")
	_file        = flag.String("file", "", "Output file to write results")
	_verbsoe     = flag.Bool("verbose", false, "Enables verbose output")
	_connections = flag.Int("connections", 10000, "Max open idle connections per target")
	_keepalive   = flag.Bool("keepalive", true, "Sets keepalive for connections")
	_workers     = flag.Int("workers", 0, "The number of initial worker routines")
	_maxworkers  = flag.Int("maxworkers", 0, "The maximum number of workers to spawn")
	_body        = flag.String("body", "", "The body of the request (either a @file or a string)")
)

var logger = log.New(ioutil.Discard, "DEBUG ", log.Lshortfile|log.Lmicroseconds)
var bodyText []byte

func main() {
	flag.Var(&_headersValue, "header", "Custom header(s)")
	flag.Parse()

	if *_verbsoe {
		logger.SetOutput(os.Stdout)
	}

	url := flag.Arg(0)
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			MaxIdleConnsPerHost: *_connections,
			IdleConnTimeout:     10 * time.Second,
			DisableKeepAlives:   !*_keepalive,
		},
		Timeout: 10 * time.Second,
	}

	core := core{
		client:  &client,
		url:     url,
		method:  *_method,
		headers: make(http.Header),
		done:    make(chan struct{}),
	}

	for _, header := range _headersValue {
		s := strings.SplitN(header, ":", 2)
		if len(s) != 2 {
			panic(fmt.Errorf("invalid header %q", header))
		}
		core.headers.Set(s[0], s[1])
	}

	if len(*_body) > 0 {
		if (*_body)[0] == '@' {
			var err error
			bodyText, err = ioutil.ReadFile((*_body)[1:])
			if err != nil {
				panic(err)
			}
		} else {
			bodyText = []byte(*_body)
		}
	}

	if _test != nil && *_test {
		issueTestRequest(&client, core.makeReq(bodyText))
		return
	}

	end := func() {
		logger.Println("Closing the done channel")
		close(core.done)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		end()
	}()

	// Run the load test
	if *_time > 0 {
		time.AfterFunc(*_time, func() {
			end()
		})
	}

	if *_file != "" {
		logger.Printf("Opening output file %q", *_file)
		f, err := os.OpenFile(*_file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			panic(err)
		}

		fmt.Fprintln(f, "timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,Latency,IdleTime,Connect")
		core.out = f
		core.outChan = make(chan string, 1)
		core.outDone = make(chan struct{})
		go core.writeOut()
	}

	core.wg.Add(1)
	go core.reportStatus()

	core.blast(newRate(*_rate, *_increment))

	// When blast returns the load test is done, so wait for all outstanding routines to close
	logger.Println("Waiting for outstanding requests")
	ticker := time.NewTicker(time.Second)
	go func() {
		for range ticker.C {
			logger.Printf("Routines: %d", atomic.LoadInt32(&core.routines))
			return
		}
	}()

	core.wg.Wait()

	// All the workers are done, so now we just need to wait until all the output is drained
	ticker.Stop()
	if core.outDone != nil && core.outChan != nil {
		close(core.outChan)
		<-core.outDone
	}

	logger.Println("Done")
	fmt.Println()
}

func (c *core) makeReq(body []byte) *http.Request {
	req, err := http.NewRequest(c.method, c.url, nil)
	if err != nil {
		panic(err)
	}

	req.Header = c.headers
	req.Body = ioutil.NopCloser(bytes.NewReader(body))
	return req
}

func (c *core) writeOut() {
	for line := range c.outChan {
		// Add the timestamp field here to ensure it only increases
		ts := time.Now().UnixNano() / int64(time.Millisecond)
		fmt.Fprintf(c.out, "%d,%s\n", ts, line)
	}

	logger.Println("Closing log writer routine")
	close(c.outDone)
}

func (c *core) reportStatus() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	totalSuccess := int32(0)
	totalErr := int32(0)

	for {
		select {
		case <-ticker.C:
			success := atomic.SwapInt32(&c.successCount, 0)
			errs := atomic.SwapInt32(&c.errCount, 0)
			routines := atomic.LoadInt32(&c.routines)
			rate := success + errs
			totalSuccess += success
			totalErr += errs
			fmt.Fprintf(os.Stderr, "Rate: %d RPS\tSuccess:%d\tErr:%d\tRoutines:%d\n", rate, totalSuccess, totalErr, routines)
		case <-c.done:
			logger.Println("Closing status reporter routine")
			return
		}
	}
}

func (c *core) issueQuery() {
	req := c.makeReq(bodyText)

	var conStart, conEnd time.Time
	trace := httptrace.ClientTrace{
		ConnectStart: func(network, addr string) {
			conStart = time.Now()
		},
		ConnectDone: func(network, addr string, err error) {
			conEnd = time.Now()
		},
	}

	reqBytes, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		panic(err)
	}

	sentBytes := len(reqBytes)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &trace))
	start := time.Now()
	res, err := c.client.Do(req)
	end := time.Now()

	latency := end.Sub(start).Truncate(time.Millisecond) / time.Millisecond
	conLatency := conEnd.Sub(conStart).Truncate(time.Millisecond) / time.Millisecond
	if conLatency < 0 {
		conLatency = 0
	}

	var failureMsg string
	const outFmt = "%d,%s,%d,%s,%s,%s,%v,%q,%d,%d,%d,%d,%d,%d,%d"

	if err != nil {
		atomic.AddInt32(&c.errCount, 1)
		failureMsg = err.Error()
		threads := atomic.LoadInt32(&c.routines)
		if c.outChan != nil {
			c.outChan <- fmt.Sprintf(
				outFmt,
				latency,          // elapsed
				"HTTP Request",   // label
				0,                // response code
				"",               // responseMessage
				"LoadTestThread", // threadName
				"text",           // dataType
				false,            // success
				failureMsg,       // failureMessage
				0,                // bytes
				sentBytes,        // sentBytes
				threads,          // grpThreads
				threads,          // allThreads,
				latency,          // latency
				0,                // idle time
				conLatency)       // connect
		}
		return
	}

	success := res.StatusCode < 400
	if !success {
		failureMsg = res.Status
	}

	if success {
		atomic.AddInt32(&c.successCount, 1)
	} else {
		atomic.AddInt32(&c.errCount, 1)
	}

	// Read the response body even if we don't log it because its needed to ensure
	// TCP connections get reused.
	bout, err := httputil.DumpResponse(res, true)
	if err != nil {
		panic(err)
	}

	closeBody(res)
	if c.outChan == nil {
		return
	}

	recvBytes := len(bout)
	threads := atomic.LoadInt32(&c.routines)
	c.outChan <- fmt.Sprintf(
		outFmt,
		latency,          // elapsed
		"HTTP Request",   // label
		res.StatusCode,   // response code
		res.Status,       // responseMessage
		"LoadTestThread", // threadName
		"text",           // dataType
		success,          // success
		failureMsg,       // failureMessage
		recvBytes,        // bytes
		sentBytes,        // sentBytes
		threads,          // grpThreads
		threads,          // allThreads,
		latency,          // latency
		0,                // idle time
		conLatency)       // connect
}

func (c *core) worker(reqChan chan struct{}) {
	defer c.wg.Done()
	defer atomic.AddInt32(&c.routines, -1)

	for range reqChan {
		c.issueQuery()
	}
}

func (c *core) blast(rate *rate) {
	reqChan := make(chan struct{})
	defer close(reqChan)

	c.wg.Add(*_workers)
	for i := 0; i < *_workers; i++ {
		go c.worker(reqChan)
	}

	for {
		rate.SetNext()
		select {
		case <-c.done:
			logger.Println("Closing the blaster routine")
			return
		default:
			select {
			case reqChan <- struct{}{}:
			default:
				if *_maxworkers > 0 && atomic.LoadInt32(&c.routines) < int32(*_maxworkers) {
					// All workers busy. Create a new worker and try again.
					atomic.AddInt32(&c.routines, 1)
					c.wg.Add(1)
					go c.worker(reqChan)
				}
			}

			// Now we need to wait for the next interval
			rate.Pause()
		}
	}
}

func closeBody(res *http.Response) {
	if err := res.Body.Close(); err != nil {
		panic(err)
	}
}

func issueTestRequest(client *http.Client, req *http.Request) {
	t := client.Transport.(*http.Transport)
	t.DisableCompression = true

	if !*_keepalive {
		req.Close = true
	}

	breq, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n", breq)
	response, err := client.Do(req)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	bres, err := httputil.DumpResponse(response, false)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", bres)
}
