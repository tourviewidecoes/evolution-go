package webhook_producer

import (
	"net/http"
	"testing"
)

func TestRetryableWebhookStatus(t *testing.T) {
	retryable := []int{0, http.StatusRequestTimeout, http.StatusTooManyRequests, 500, 502, 503, 504}
	for _, status := range retryable {
		if !isRetryableWebhookStatus(status) {
			t.Fatalf("expected status %d to be retryable", status)
		}
	}

	permanent := []int{400, 401, 403, 404, 409, 422}
	for _, status := range permanent {
		if isRetryableWebhookStatus(status) {
			t.Fatalf("expected status %d to be permanent", status)
		}
	}
}

func TestWebhookCircuitOpensAfterThreshold(t *testing.T) {
	p := &webhookProducer{circuits: make(map[string]circuitState)}
	url := "https://example.test/webhook"

	for i := 0; i < webhookCircuitThreshold; i++ {
		p.recordFailure(url)
	}

	if p.allowTarget(url) {
		t.Fatal("expected circuit to be open after failure threshold")
	}
}
