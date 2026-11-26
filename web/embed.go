package web

import "embed"

//go:embed static/index.html static/app.js
var DashboardFS embed.FS
