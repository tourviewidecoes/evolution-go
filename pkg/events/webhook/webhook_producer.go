package webhook_producer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	producer_interfaces "github.com/evolution-foundation/evolution-go/pkg/events/interfaces"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

const (
	webhookRequestTimeout      = 45 * time.Second
	webhookMaxRetryInterval    = 2 * time.Minute
	webhookMaxConcurrentSends  = 4
)

// Backpressure global por processo. Em degradação do receptor, cada evento
// costumava abrir uma goroutine HTTP sem limite; bursts + retries podiam manter
// dezenas de requests simultâneos e amplificar a saturação do backend receptor.
// A fila fica no processo Evolution e preserva os eventos/retries, mas somente
// um número pequeno de POSTs pode estar em voo ao mesmo tempo.
var webhookSendSlots = make(chan struct{}, webhookMaxConcurrentSends)

type webhookProducer struct {
	url           string
	loggerWrapper *logger_wrapper.LoggerManager
	client        *http.Client
}

func NewWebhookProducer(
	url string,
	loggerWrapper *logger_wrapper.LoggerManager,
) producer_interfaces.Producer {
	return &webhookProducer{
		url:           url,
		loggerWrapper: loggerWrapper,
		client:        &http.Client{Timeout: webhookRequestTimeout},
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

	if p.url != "" {
		go p.sendWebhookWithRetry(p.url, payload, 5, 30*time.Second, userID)
	}
	if webhookUrl != "" {
		go p.sendWebhookWithRetry(webhookUrl, payload, 5, 30*time.Second, userID)
	}

	return nil
}

func webhookRetryDelay(initial time.Duration, failedAttempt int) time.Duration {
	if initial <= 0 {
		return 0
	}
	if failedAttempt < 1 {
		failedAttempt = 1
	}

	delay := initial
	for attempt := 1; attempt < failedAttempt; attempt++ {
		if delay >= webhookMaxRetryInterval/2 {
			return webhookMaxRetryInterval
		}
		delay *= 2
	}
	if delay > webhookMaxRetryInterval {
		return webhookMaxRetryInterval
	}
	return delay
}

func (p *webhookProducer) sendWebhookWithRetry(url string, body []byte, maxRetries int, retryInterval time.Duration, userID string) {
	for i := 0; i < maxRetries; i++ {
		err, responseBody, statusCode := p.sendWebhook(url, body, userID)
		if err == nil {
			p.loggerWrapper.GetLogger(userID).LogInfo("[%s] webhook sent successfully - url: %s, status: %d, response: %s", userID, url, statusCode, string(responseBody))
			return
		}

		attempt := i + 1
		if attempt >= maxRetries {
			p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook failed - url: %s, attempt: %d, error: %v", userID, url, attempt, err)
			break
		}

		delay := webhookRetryDelay(retryInterval, attempt)
		p.loggerWrapper.GetLogger(userID).LogWarn("[%s] webhook failed - url: %s, attempt: %d, retry_in: %s, error: %v", userID, url, attempt, delay, err)
		time.Sleep(delay)
	}
	p.loggerWrapper.GetLogger(userID).LogError("[%s] webhook failed after maximum retries - url: %s", userID, url)
}

func (p *webhookProducer) sendWebhook(url string, body []byte, userID string) (error, []byte, int) {
	webhookSendSlots <- struct{}{}
	defer func() { <-webhookSendSlots }()

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err, nil, 0
	}

	req.Header.Set("Content-Type", "application/json")

	client := p.client
	if client == nil {
		client = &http.Client{Timeout: webhookRequestTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err, nil, 0
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("erro ao ler resposta: %v", err), nil, 0
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
