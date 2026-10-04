package webhook_producer

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewWebhookProducerUsesBoundedHTTPClient(t *testing.T) {
	producer := NewWebhookProducer("", nil)
	webhook, ok := producer.(*webhookProducer)
	if !ok {
		t.Fatalf("expected *webhookProducer, got %T", producer)
	}
	if webhook.client == nil {
		t.Fatal("expected configured HTTP client")
	}
	if webhook.client.Timeout != webhookRequestTimeout {
		t.Fatalf("expected timeout %s, got %s", webhookRequestTimeout, webhook.client.Timeout)
	}
}

func TestSendWebhookHonorsClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	webhook := &webhookProducer{
		client: &http.Client{Timeout: 25 * time.Millisecond},
	}

	start := time.Now()
	err, _, _ := webhook.sendWebhook(server.URL, []byte(`{"event":"Message"}`), "test")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("request timeout was not bounded: %s", elapsed)
	}
}
