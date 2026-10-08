package event_types

import (
	"reflect"
	"testing"
)

func TestOrQiFlowRuntimeEvents(t *testing.T) {
	want := []string{MESSAGE, SEND_MESSAGE, READ_RECEIPT, CONNECTION, GROUP}
	got := OrQiFlowRuntimeEvents()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected OrQiFlow runtime events: got %v want %v", got, want)
	}
	for _, event := range got {
		if !IsEventType(event) {
			t.Fatalf("OrQiFlow event %q is not a valid event type", event)
		}
	}
}
