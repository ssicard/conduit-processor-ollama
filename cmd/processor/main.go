//go:build wasm

package main

import (
	sdk "github.com/conduitio/conduit-processor-sdk"
	ollama "github.com/conduitio/conduit-processor-template"
)

func main() {
	sdk.Run(ollama.NewProcessor())
}
