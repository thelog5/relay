// Package web embeds the dashboard so the api binary is self-contained.
package web

import _ "embed"

// Dashboard is web/dashboard.html.
//
//go:embed dashboard.html
var Dashboard []byte
