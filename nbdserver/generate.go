package main

// VolumeController client
//go:generate go-raml client --ramlfile ../volumecontroller/volumeController.raml --dir clients/volumecontroller/ --package volumecontroller

// VolumeController stub
//go:generate go-raml server --ramlfile ../volumecontroller/volumeController.raml --dir stubs/volumecontroller --package volumecontroller --import-path "github.com/g8os/blockstor/nbdserver/stubs/volumecontroller"
