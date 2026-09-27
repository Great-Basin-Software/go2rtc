package core

import (
	"errors"
	"testing"
	"time"
)

// Done stores the error before it releases Wait, so Wait returns it: the
// race detector flags the old order, where Wait read it while Done wrote it.
func TestWaiterWaitReturnsDoneError(t *testing.T) {
	want := errors.New("closed")
	for i := 0; i < 200; i++ {
		w := &Waiter{}
		got := make(chan error, 1)
		go func() { got <- w.Wait() }()
		for { // until Wait has registered
			w.mu.Lock()
			started := w.state > 0
			w.mu.Unlock()
			if started {
				break
			}
			time.Sleep(time.Microsecond)
		}
		w.Done(want)
		if err := <-got; err != want {
			t.Fatalf("run %d: Wait returned %v, want %v", i, err, want)
		}
	}
}
