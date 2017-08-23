package db

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2"
)

var dbSession *mgo.Session

func initializeDB(url string) error {

	if dbSession != nil {
		log.Debug("DB Session is already initialized")
		return nil
	}

	var err error
	dbSession, err = mgo.Dial(url)

	if err != nil {
		log.Errorf("Failed to initialize DB connection: %s", err.Error())
		return err
	}

	return nil
}

// Connect ensures a mongo DB connection is initialized.
func Connect(url string) {
	if dbSession != nil {
		return
	}

	for {
		err := initializeDB(url)
		if err == nil {
			break
		}
		log.Debugf("Failed to connect to DB (%s), retrying in 5 seconds...", url)
		time.Sleep(5 * time.Second)
	}

	log.Info("Initialized mongo connection")
}

func Close() {
	if dbSession == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.Debug("Recovered while closing DB session.", r)
		}
	}()

	dbSession.Close()
}
