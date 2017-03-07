package main

// VolumeController client
//go:generate go-raml client --ramlfile ../volumecontroller/volumeController.raml --dir clients/volumecontroller/ --package volumecontroller

// VolumeController stub
//go:generate go-raml server --ramlfile ../volumecontroller/volumeController.raml --dir stubs/volumecontroller --package volumecontroller --import-path "github.com/g8os/blockstor/nbdserver/stubs/volumecontroller"

// VolumeController HTML docs
//go:generate raml2html --input ../volumecontroller/volumeController.raml --output ../volumecontroller/volumeController.html

// Storage backend clients
//go:generate go-raml client --ramlfile clients/storagebackendcontroller/storagebackend.raml --dir clients/storagebackendcontroller/ --package storagebackendcontroller

// Storage backend stub
//go:generate go-raml server --ramlfile clients/storagebackendcontroller/storagebackend.raml --dir stubs/storagebackendcontroller --package storagebackendcontroller --import-path "github.com/g8os/blockstor/nbdserver/stubs/storagebackendcontroller"
