package db

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/mgo.v2"
)

//EnsureIndex make sure indices are created on certain collection.
func EnsureIndex(collectionName string, index mgo.Index) {
	session := GetSession()
	defer session.Close()

	c := GetCollection(session, collectionName)

	// Make sure idices are created.
	if err := c.EnsureIndex(index); err == nil {
		log.Debugf("Ensured \"%s\" collection indices", collectionName)
	} else {
		// Important: Mongo3 doesn't support DropDups!
		log.Fatalf("Failed to create index on collection \"%s\": %s. Aborting", collectionName, err.Error())
	}
}

//GetCollection return collection.
func GetCollection(session *mgo.Session, collectionName string) *mgo.Collection {
	return session.DB(DB_NAME).C(collectionName)
}

//GetSession blocking call until session is ready.
func GetSession() *mgo.Session {
	for {
		if session := NewSession(); session != nil {
			return session
		}

		time.Sleep(1 * time.Second)
	}
}

//IsDup checks if an error means it's a duplicate
func IsDup(err error) bool {
	return (err == ErrDuplicate || mgo.IsDup(err))
}

// IsNotFound checks if an error indicates that a record does not exist
func IsNotFound(err error) bool {
	return err == mgo.ErrNotFound
}
