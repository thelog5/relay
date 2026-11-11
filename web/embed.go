// Package web embeds the dashboard and its images so the api binary is
// self-contained.
package web

import (
	"embed"
	"io/fs"
)

// Dashboard is web/dashboard.html.
//
//go:embed dashboard.html
var Dashboard []byte

//go:embed static
var static embed.FS

// Static holds the logo and icons, served under /static/.
var Static, _ = fs.Sub(static, "static")
