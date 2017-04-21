package ardb

import (
	log "github.com/glendc/go-mini-log"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LDebug)
}
