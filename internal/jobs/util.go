package jobs

import (
	"bytes"
	"context"
	"io"
	"time"
)

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// sleepCtx waits for d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
