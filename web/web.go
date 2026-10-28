// Package web embeds the visualizer's static files.
package web

import "embed"

// FS holds index.html, style.css and app.js at its root.
//
//go:embed index.html style.css app.js
var FS embed.FS
