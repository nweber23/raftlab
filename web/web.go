// Package web embeds the visualizer's static files.
package web

import "embed"

// FS holds index.html, style.css, app.js and fonts/ at its root.
//
//go:embed index.html style.css app.js fonts/*.woff2
var FS embed.FS
