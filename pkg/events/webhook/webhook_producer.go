package webhook_producer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	producer_interfaces "github.com/evolution-foundation/evolution-go/pkg/events/interfaces"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

const (
	maxWebhookRetries       = 3
	webhookRequestTimeout   = 8 * time.Second
	webhookCircuitThreshold = 3
	webhookCircuitCooldown  = 60 * time.Second
	webhookWorkers          = 8
)

type circuitState struct {
	failures  int
	openUntil time.Time
}

type webhookProducer struct {
	url           string
	loggerWrapper *logger_wrapper.LoggerManager
	client        *http.Client
	slots         chan struct{}
	mu            sync.Mutex
	circuits      map[string]circuitState
}

func NewWebhookProducer(
	url string,
	loggerWrapper *logger_wrapper.LoggerManager,
) producer_interfaces.Producer {
	return &webhookProducer{
		url:           url,
		loggerWrapper: loggerWrapper,
		client:        &http.Client{Timeout: webhookRequestTimeout},
		slots:         make(chan struct{}, webhookWorkers),
		circuits:      make(map[string]circuitState),
	}
}

func (p *webhookProducer) Produce(
	queueName string,
	payload []byte,
	webhookUrl string,
	userID string,
) error {
	splitQueue := strings.Split(queueName, ".")

	if len(splitQueue) < 2 {
		return nil
	}

	for _, target := range []string{p.url, webhookUrl} {
		if target == "" || !p.allowTarget(target) {
			continue
		}

		// Backpressure: never create an unbounded number of retry goroutines.
		// WhatsApp messages are persisted before fan-out, so when the delivery
		// layer is saturated we protect the process/Supabase and let recovery
		// reconcile instead of amplifying an outage.
		select {
		case p.slots <- struct{}{}:
			go func(url string) {
				defer func() { <-p.slots }()
				p.sendWebhookWithRetry(url, payload, userID)
			}(target)
		default:
			p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook delivery saturated; dropping fan-out attempt for url: %s", userID, target)
		}
	}

	return nil
}

func (p *webhookProducer) allowTarget(url string) bool {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.circuits[url]
	if state.openUntil.After(now) {
		return false
	}
	if !state.openUntil.IsZero() {
		state.openUntil = time.Time{}
		state.failures = 0
		p.circuits[url] = state
	}
	return true
}

func (p *webhookProducer) recordSuccess(url string) {
	p.mu.Lock()
	delete(p.circuits, url)
	p.mu.Unlock()
}

func (p *webhookProducer) recordFailure(url string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.circuits[url]
	state.failures++
	if state.failures >= webhookCircuitThreshold {
		state.openUntil = time.Now().Add(webhookCircuitCooldown)
		state.failures = 0
	}
	p.circuits[url] = state
}

func isRetryableWebhookStatus(statusCode int) bool {
	return statusCode == 0 || statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func (p *webhookProducer) sendWebhookWithRetry(url string, body []byte, userID string) {
	backoff := time.Second

	for attempt := 1; attempt <= maxWebhookRetries; attempt++ {
		err, responseBody, statusCode := p.sendWebhook(url, body)
		if err == nil {
			p.recordSuccess(url)
			p.loggerWrapper.GetLogger(userID).LogInfo("[%s] webhook sent successfully - url: %s, status: %d, response: %s", userID, url, statusCode, string(responseBody))
			return
		}

		if !isRetryableWebhookStatus(statusCode) {
			// 4xx other than 408/429 are permanent for this payload/config.
			// Retrying them only multiplies load and cannot make them succeed.
			p.recordFailure(url)
			p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook permanent failure - url: %s, status: %d, error: %v", userID, url, statusCode, err)
			return
		}

		p.recordFailure(url)
		p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook transient failure - url: %s, attempt: %d/%d, status: %d, error: %v", userID, url, attempt, maxWebhookRetries, statusCode, err)

		if !p.allowTarget(url) || attempt == maxWebhookRetries {
			return
		}

		time.Sleep(backoff)
		backoff *= 2
	}
}

func (p *webhookProducer) sendWebhook(url string, body []byte) (error, []byte, int) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return err, nil, 0
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return err, nil, 0
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return fmt.Errorf("erro ao ler resposta: %v", err), nil, resp.StatusCode
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("received non-2xx response: " + resp.Status), responseBody, resp.StatusCode
	}

	return nil, responseBody, resp.StatusCode
}

// CreateGlobalQueues não faz nada para webhook producer
func (p *webhookProducer) CreateGlobalQueues() error {
	return nil
}
