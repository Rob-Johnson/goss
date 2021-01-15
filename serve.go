package goss

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	oldLog "log"

	"github.com/aelsabbahy/goss/outputs"
	"github.com/aelsabbahy/goss/resource"
	"github.com/aelsabbahy/goss/system"
	"github.com/aelsabbahy/goss/util"
	"github.com/fatih/color"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	zipkin "github.com/openzipkin/zipkin-go"
	logReporter "github.com/openzipkin/zipkin-go/reporter/log"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
)

const traceIDKey string = "traceID"
const requestLoggerKey string = "requestLogger"

func Serve(c *util.Config) error {
	endpoint := c.Endpoint
	health, err := newHealthHandler(c)
	if err != nil {
		return err
	}
	http.Handle(endpoint, health)
	log.SetFormatter(&log.JSONFormatter{})
	log.Printf("Starting to listen on: %s", c.ListenAddress)

	tracer, err := noopZipkinTracer()
	if err != nil {
		log.Printf("error creating tracer - starting with noop %s", err)
		http.ListenAndServe(c.ListenAddress, nil)
	} else {
		http.ListenAndServe(c.ListenAddress, nethttp.Middleware(tracer, WithRequestLogger(http.DefaultServeMux)))
	}
	return nil
}

func noopZipkinTracer() (opentracing.Tracer, error) {
	logger := oldLog.New(ioutil.Discard, "", oldLog.LstdFlags)
	reporter := logReporter.NewReporter(logger)
	t, err := zipkin.NewTracer(
		reporter,
	)
	if err != nil {
		return nil, err
	}
	tracer := zipkinot.Wrap(t)

	return tracer, err
}

func newHealthHandler(c *util.Config) (*healthHandler, error) {
	color.NoColor = true
	cache := cache.New(c.Cache, 30*time.Second)

	cfg, err := getGossConfig(c.Vars, c.VarsInline, c.Spec)
	if err != nil {
		return nil, err
	}

	output, err := getOutputer(c.NoColor, c.OutputFormat)
	if err != nil {
		return nil, err
	}

	health := &healthHandler{
		c:             c,
		gossConfig:    *cfg,
		sys:           system.New(c.PackageManager),
		outputer:      output,
		cache:         cache,
		gossMu:        &sync.Mutex{},
		maxConcurrent: c.MaxConcurrent,
	}
	return health, nil
}

type res struct {
	body       bytes.Buffer
	statusCode int
}
type healthHandler struct {
	c             *util.Config
	gossConfig    GossConfig
	sys           *system.System
	outputer      outputs.Outputer
	cache         *cache.Cache
	gossMu        *sync.Mutex
	maxConcurrent int
}

func (h healthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	outputFormat, outputer, err := h.negotiateResponseContentType(r)
	if err != nil {
		log.Printf("Warn: Using process-level output-format. %s", err)
		outputFormat = h.c.OutputFormat
		outputer = h.outputer
	}
	negotiatedContentType := h.responseContentType(outputFormat)

	resp := h.processAndEnsureCached(r.Context(), negotiatedContentType, outputer)
	w.Header().Set(http.CanonicalHeaderKey("Content-Type"), negotiatedContentType)
	w.Header().Set("X-Paasta-Pod", os.Getenv("POD_NAME"))
	w.Header().Set("X-Paasta-Host", os.Getenv("PAASTA_HOST"))
	w.WriteHeader(resp.statusCode)
	resp.body.WriteTo(w)

}

func (h healthHandler) processAndEnsureCached(ctx context.Context, negotiatedContentType string, outputer outputs.Outputer) res {
	cacheKey := fmt.Sprintf("res:%s", negotiatedContentType)
	tmp, found := h.cache.Get(cacheKey)
	if found {
		return tmp.(res)
	}

	h.gossMu.Lock()
	defer h.gossMu.Unlock()
	tmp, found = h.cache.Get(cacheKey)
	if found {
		LoggerFromContext(ctx).Printf("Returning cached[%s].", cacheKey)
		return tmp.(res)
	}

	LoggerFromContext(ctx).Printf("Stale cache[%s], running tests", cacheKey)
	resp := h.runValidate(ctx, outputer)
	h.cache.SetDefault(cacheKey, resp)
	return resp
}

func (h healthHandler) runValidate(ctx context.Context, outputer outputs.Outputer) res {
	h.sys = system.New(h.c.PackageManager)
	iStartTime := time.Now()
	out := validate(h.sys, h.gossConfig, h.maxConcurrent)
	var b bytes.Buffer
	outputConfig := util.OutputConfig{
		FormatOptions: h.c.FormatOptions,
	}
	exitCode := outputer.Output(&b, out, iStartTime, outputConfig)
	resp := res{
		body: b,
	}
	if exitCode == 0 {
		resp.statusCode = http.StatusOK
	} else {
		resp.statusCode = http.StatusServiceUnavailable
	}
	return resp
}

const (
	// https://en.wikipedia.org/wiki/Media_type
	mediaTypePrefix = "application/vnd.goss-"
)

func (h healthHandler) negotiateResponseContentType(r *http.Request) (string, outputs.Outputer, error) {
	acceptHeader := r.Header[http.CanonicalHeaderKey("Accept")]
	var outputer outputs.Outputer
	outputName := ""
	for _, acceptCandidate := range acceptHeader {
		acceptCandidate = strings.TrimSpace(acceptCandidate)
		if strings.HasPrefix(acceptCandidate, mediaTypePrefix) {
			outputName = strings.TrimPrefix(acceptCandidate, mediaTypePrefix)
		} else if strings.EqualFold("application/json", acceptCandidate) || strings.EqualFold("text/json", acceptCandidate) {
			outputName = "json"
		} else {
			outputName = ""
		}
		var err error
		outputer, err = outputs.GetOutputer(outputName)
		if err != nil {
			continue
		}
	}
	if outputer == nil {
		return "", nil, fmt.Errorf("Accept header on request missing or invalid. Accept header: %v", acceptHeader)
	}

	return outputName, outputer, nil
}

func (h healthHandler) responseContentType(outputName string) string {
	if outputName == "json" {
		return "application/json"
	}
	return fmt.Sprintf("%s%s", mediaTypePrefix, outputName)
}

func (h healthHandler) renderBody(results <-chan []resource.TestResult, outputer outputs.Outputer) (int, bytes.Buffer) {
	outputConfig := util.OutputConfig{
		FormatOptions: h.c.FormatOptions,
	}
	var b bytes.Buffer
	exitCode := outputer.Output(&b, results, time.Now(), outputConfig)
	return exitCode, b
}

func extractTraceID(ctx context.Context) (string, bool) {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return "", false
	}
	sctx, ok := sp.Context().(zipkinot.SpanContext)
	if !ok {
		return "", false
	}
	return sctx.TraceID.String(), true
}

//WithRequestLogger returns a http.HandlerFunc to be used
//as middleware creating request-scoped loggers.
func WithRequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		traceID, found := extractTraceID(r.Context())
		if !found {
			traceID = ""
		}
		commonLogFields := log.Fields{
			traceIDKey: traceID,
		}
		requestLogger := LoggerFromContext(ctx).WithFields(commonLogFields)
		ctx = context.WithValue(ctx, requestLoggerKey, requestLogger)

		recorder := &StatusRecorder{
			ResponseWriter: w,
			Status:         200,
		}

		startTime := time.Now()
		next.ServeHTTP(recorder, r.WithContext(ctx))
		finTime := time.Now()

		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		logData := log.Fields{
			"timestamp":  startTime.Format("02/Jan/2006:15:04:05 -0700"),
			"client":     host,
			"method":     r.Method,
			"uri":        r.RequestURI,
			"proto":      r.Proto,
			"referer":    r.Referer(),
			"user-agent": r.UserAgent(),
			"duration":   finTime.Sub(startTime).String(),
			"headers":    r.Header,
			"status":     recorder.Status,
			"podIP":      os.Getenv("PAASTA_POD_IP"),
			"serverIP":   os.Getenv("PAASTA_HOST"),
		}
		requestLogger.WithFields(logData).Infoln()
	})
}

//StatusRecorder implements an http.ResponseWriter which tracks status code
//so that we can write access logs
type StatusRecorder struct {
	http.ResponseWriter
	Status int
}

func (r *StatusRecorder) WriteHeader(status int) {
	r.Status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *StatusRecorder) Write(p []byte) (int, error) {
	return r.ResponseWriter.Write(p)
}

func LoggerFromContext(ctx context.Context) *log.Entry {
	logger := ctx.Value(requestLoggerKey)

	if logger == nil {
		l := log.Logger{
			Out:       os.Stdout,
			Formatter: new(log.JSONFormatter),
			Hooks:     make(log.LevelHooks),
			Level:     log.DebugLevel,
		}
		return log.NewEntry(&l)
	}
	l, ok := logger.(*log.Entry)
	if !ok {
		panic("failed to extract logger from context")
	}
	return l
}
