package stats

import (
	"time"

	"sync"

	"github.com/paulbellamy/ratecounter"
)

var lock = sync.RWMutex{}

type stat struct {
	readReq  *ratecounter.RateCounter
	writeReq *ratecounter.RateCounter
}

func newStat() stat {
	return stat{
		readReq:  ratecounter.NewRateCounter(time.Hour),
		writeReq: ratecounter.NewRateCounter(time.Hour),
	}
}

type namespaceStatMap map[string]stat

var golbalNamespaceStat namespaceStatMap

func init() {
	golbalNamespaceStat = namespaceStatMap{}
}

// AddNamespace create a rate counter this a namespace
func AddNamespace(label string) {
	_, ok := golbalNamespaceStat[label]
	if ok {
		return
	}

	golbalNamespaceStat[label] = newStat()
	return
}

// Incr increment the read request counter for a namespace
func IncrRead(label string) {
	lock.Lock()
	defer lock.Unlock()
	stat, ok := golbalNamespaceStat[label]
	if !ok {
		AddNamespace(label)
		stat = golbalNamespaceStat[label]
	}

	stat.readReq.Incr(1)
}

// Incr increment the write request counter for a namespace
func IncrWrite(label string) {
	lock.Lock()
	defer lock.Unlock()
	stat, ok := golbalNamespaceStat[label]
	if !ok {
		AddNamespace(label)
		stat = golbalNamespaceStat[label]
	}

	stat.writeReq.Incr(1)
}

// Rate return the numer of request per hour for a namespace
func Rate(label string) (read, write int64) {
	lock.RLock()
	defer lock.RUnlock()
	stat, ok := golbalNamespaceStat[label]
	if !ok {
		AddNamespace(label)
		stat = golbalNamespaceStat[label]
	}

	return stat.readReq.Rate(), stat.writeReq.Rate()
}
