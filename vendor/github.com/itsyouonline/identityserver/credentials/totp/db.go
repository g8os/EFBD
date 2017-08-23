package totp

import (
	"net/http"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	log "github.com/Sirupsen/logrus"
	"github.com/itsyouonline/identityserver/db"
)

const (
	mongoCollectionName = "totp"
)

type userSecret struct {
	Username string
	Secret   string
}

// InitModels initializes models in mongo, if required.
func InitModels() {
	index := mgo.Index{
		Key:      []string{"username"},
		Unique:   true,
		DropDups: true,
	}

	db.EnsureIndex(mongoCollectionName, index)
}

//Manager stores and validates passwords
type Manager struct {
	session    *mgo.Session
	collection *mgo.Collection
}

func getTotpCollection(session *mgo.Session) *mgo.Collection {
	return db.GetCollection(session, mongoCollectionName)
}

//NewManager creates a new Manager
func NewManager(r *http.Request) *Manager {
	session := db.GetDBSession(r)
	return &Manager{
		session:    session,
		collection: getTotpCollection(session),
	}
}

//Validate checks the totp code for a specific username
func (pwm *Manager) Validate(username, securityCode string) (bool, error) {

	var storedSecret userSecret
	if err := pwm.collection.Find(bson.M{"username": username}).One(&storedSecret); err != nil {
		if err == mgo.ErrNotFound {
			log.Debug("No totpsecret found for this user")
			return false, nil
		}
		log.Debug(err)
		return false, err
	}
	token := TokenFromSecret(storedSecret.Secret)
	match := token.Validate(securityCode)
	return match, nil
}

func (pwm *Manager) HasTOTP(username string) (hastoken bool, err error) {
	hastoken = false
	count, err := pwm.collection.Find(bson.M{"username": username}).Count()
	if err != nil {
		count = 0
		return
	}
	hastoken = count != 0
	return
}

// Save stores a secret for a specific username.
func (pwm *Manager) Save(username, secret string) error {
	//TODO: username and secret validation

	storedSecret := userSecret{Username: username, Secret: secret}

	_, err := pwm.collection.Upsert(bson.M{"username": username}, storedSecret)

	return err
}

func (pwm *Manager) Remove(username string) error {
	return pwm.collection.Remove(bson.M{"username": username})
}

func (pwm *Manager) GetSecret(username string) (err error, secret userSecret) {
	err = pwm.collection.Find(bson.M{"username": username}).One(&secret)
	return err, secret
}
