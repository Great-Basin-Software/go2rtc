package nest

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/webrtc"
)

// The watchdog's video count sees only video tracks: audio still arriving
// must not hide a session whose picture stopped.
func TestVideoPacketsCountsOnlyVideo(t *testing.T) {
	video := core.NewReceiver(&core.Media{Kind: core.KindVideo}, &core.Codec{Name: core.CodecH264})
	audio := core.NewReceiver(&core.Media{Kind: core.KindAudio}, &core.Codec{Name: core.CodecOpus})
	conn := &webrtc.Conn{}
	conn.Receivers = []*core.Receiver{video, audio}
	c := &WebRTCClient{conn: conn}

	video.Input(&core.Packet{Payload: []byte{1}})
	video.Input(&core.Packet{Payload: []byte{1}})
	for i := 0; i < 50; i++ {
		audio.Input(&core.Packet{Payload: []byte{1}})
	}
	if got := c.videoPackets(); got != 2 {
		t.Fatalf("videoPackets = %d, want 2 (audio packets must not count)", got)
	}
}
