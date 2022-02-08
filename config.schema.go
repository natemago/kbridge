package kbridge

import (
	_ "embed"
)

//go:embed schemas/config.json
var ConfigJSONSchema string
