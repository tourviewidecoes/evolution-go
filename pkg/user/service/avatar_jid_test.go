package user_service

import "testing"

func TestAvatarLookupJIDUsesCanonicalWhatsAppUserJID(t *testing.T) {
	jid, err := avatarLookupJID("5515981122710")
	if err != nil {
		t.Fatalf("avatarLookupJID returned error: %v", err)
	}
	if got := jid.String(); got != "5515981122710@s.whatsapp.net" {
		t.Fatalf("avatarLookupJID = %q, want canonical digits-only JID", got)
	}
	if jid.User == "" || jid.User[0] == '+' {
		t.Fatalf("avatarLookupJID user must not include '+': %q", jid.User)
	}
}
