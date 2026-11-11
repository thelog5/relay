package api

import "time"

// Idle mode lets a serverless Postgres such as Neon scale to zero. With
// Config.IdleAfter set, the api stops its own periodic database work (the
// dashboard's stats tick and the health check's database ping) once nothing
// has happened for that long, and the reconciler and purger skip their
// passes too (cmd/api passes them Idle).
//
// Activity is an open dashboard, a job being created or retried, a failed
// stats snapshot, or a snapshot that still sees unfinished jobs. So the api
// only goes quiet after Postgres has held no unfinished jobs for IdleAfter,
// and only a request to this api can add one, which wakes it first.

// touch records activity now.
func (s *Server) touch() { s.lastActive.Store(time.Now().UnixNano()) }

// Idle reports whether IdleAfter is set and nothing has happened for that
// long. While it is true, Postgres holds no unfinished jobs.
func (s *Server) Idle() bool {
	if s.cfg.IdleAfter <= 0 {
		return false
	}
	return time.Since(time.Unix(0, s.lastActive.Load())) > s.cfg.IdleAfter
}
