package webhook_producer

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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

func TestWebhookRetryDelayUsesExponentialBackoffWithCap(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 30 * time.Second},
		{attempt: 2, want: 60 * time.Second},
		{attempt: 3, want: 2 * time.Minute},
		{attempt: 4, want: 2 * time.Minute},
		{attempt: 8, want: 2 * time.Minute},
	}

	for _, tt := range tests {
		if got := webhookRetryDelay(30*time.Second, tt.attempt); got != tt.want {
			t.Fatalf("attempt %d: expected %s, got %s", tt.attempt, tt.want, got)
		}
	}
}

func TestWebhookRetryDelayHandlesNonPositiveInput(t *testing.T) {
	if got := webhookRetryDelay(0, 1); got != 0 {
		t.Fatalf("expected zero delay, got %s", got)
	}
}


func TestSendWebhookCapsConcurrentRequests(t *testing.T) {
	var current int32
	var maxSeen int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		now := atomic.AddInt32(&current, 1)
		for {
			prev := atomic.LoadInt32(&maxSeen)
			if now <= prev || atomic.CompareAndSwapInt32(&maxSeen, prev, now) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	webhook := &webhookProducer{
		client: &http.Client{Timeout: time.Second},
	}

	var wg sync.WaitGroup
	for i := 0; i < webhookMaxConcurrentSends*3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err, _, _ := webhook.sendWebhook(server.URL, []byte(`{"event":"Message"}`), "test")
			if err != nil {
				t.Errorf("unexpected send error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := int(atomic.LoadInt32(&maxSeen)); got > webhookMaxConcurrentSends {
		t.Fatalf("expected at most %d concurrent sends, got %d", webhookMaxConcurrentSends, got)
	}
}
