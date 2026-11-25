package supervisor

import (
	"testing"

	"github.com/VoltTech21/reostream/internal/config"
)

func TestPresenceStreamChangeRestartsAffectedEntries(t *testing.T) {
	cam := config.Camera{
		Name: "doorbell", Address: "192.0.2.20", Battery: true,
		Streams: []string{"main", "sub"}, WakeOnRTSP: true,
	}
	main := newEntry(cam, "main")
	sub := newEntry(cam, "sub")
	cam.PresenceStream = "sub"
	if sameStream(main, cam) {
		t.Fatal("main remained unchanged after losing presence-stream ownership")
	}
	if sameStream(sub, cam) {
		t.Fatal("sub remained unchanged after gaining presence-stream ownership")
	}
}
