package registry

import (
	"errors"
	"net/http"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/itsyouonline/identityserver/db"
)

const (
	mongoRegistryCollectionName = "registry"
)

//ErrUsernameOrGlobalIDRequired is used to indicate that no username of globalid were specified
var ErrUsernameOrGlobalIDRequired = errors.New("Username or globalid is required")

//ErrUsernameAndGlobalIDAreMutuallyExclusive is the error given when both a username and a globalid were given
var ErrUsernameAndGlobalIDAreMutuallyExclusive = errors.New("Username and globalid can not both be specified")

//InitModels initialize models in mongo, if required.
func InitModels() {
	index := mgo.Index{
		Key:    []string{"username"},
		Unique: true,
	}

	db.EnsureIndex(mongoRegistryCollectionName, index)

	index = mgo.Index{
		Key:    []string{"globalid"},
		Unique: true,
	}

	db.EnsureIndex(mongoRegistryCollectionName, index)

	index = mgo.Index{
		Key:    []string{"entries.key"},
		Unique: true,
	}

	db.EnsureIndex(mongoRegistryCollectionName, index)

}

//Manager is used to store KeyValuePairs in a user or organization registry
type Manager struct {
	session *mgo.Session
}

//NewManager creates and initializes a new Manager
func NewManager(r *http.Request) *Manager {
	session := db.GetDBSession(r)
	return &Manager{
		session: session,
	}
}

func (m *Manager) getRegistryCollection() *mgo.Collection {
	return db.GetCollection(m.session, mongoRegistryCollectionName)
}

func validateUsernameAndGlobalID(username, globalid string) (err error) {
	if username == "" && globalid == "" {
		err = ErrUsernameOrGlobalIDRequired
	}
	if username != "" && globalid != "" {
		err = ErrUsernameAndGlobalIDAreMutuallyExclusive
	}
	return
}

func createSelector(username, globalid, key string) (selector bson.M, err error) {
	err = validateUsernameAndGlobalID(username, globalid)
	if err != nil {
		return
	}
	if username != "" {
		selector = bson.M{"username": username, "entries.key": key}
	} else {
		selector = bson.M{"globalid": globalid, "entries.key": key}
	}
	return
}

//DeleteRegistryEntry deletes a registry entry
// Either a username or a globalid needs to be given
// If the key does not exist, no error is returned
func (m *Manager) DeleteRegistryEntry(username string, globalid string, key string) (err error) {
	selector, err := createSelector(username, globalid, key)
	if err != nil {
		return
	}

	_, err = m.getRegistryCollection().UpdateAll(
		selector,
		bson.M{"$pull": bson.M{"entries": bson.M{"key": key}}})

	return
}

//UpsertRegistryEntry updates or inserts a registry entry
// Either a username or a globalid needs to be given
func (m *Manager) UpsertRegistryEntry(username string, globalid string, registryEntry RegistryEntry) (err error) {
	selector, err := createSelector(username, globalid, registryEntry.Key)
	if err != nil {
		return
	}
	result, err := m.getRegistryCollection().UpdateAll(selector, bson.M{"$set": bson.M{"entries.$.value": registryEntry.Value}})
	if err == nil && result.Updated == 0 {
		//Negate the selector on push so it is never pushed twice
		if username != "" {
			selector = bson.M{"username": username, "entries.key": bson.M{"$ne": registryEntry.Key}}
		} else {
			selector = bson.M{"globalid": globalid, "entries.key": bson.M{"$ne": registryEntry.Key}}
		}
		_, err = m.getRegistryCollection().Upsert(selector, bson.M{"$push": bson.M{"entries": &registryEntry}})
	}
	return
}

//ListRegistryEntries gets all registry entries for a user or organization
func (m *Manager) ListRegistryEntries(username string, globalid string) (registryEntries []RegistryEntry, err error) {
	var selector bson.M
	if username != "" {
		selector = bson.M{"username": username}
	} else {
		selector = bson.M{"globalid": globalid}
	}
	result := struct {
		Entries []RegistryEntry
	}{}
	err = m.getRegistryCollection().Find(selector).Select(bson.M{"entries": 1}).One(&result)
	if err == mgo.ErrNotFound {
		err = nil
		registryEntries = []RegistryEntry{}
		return
	}
	registryEntries = result.Entries
	return
}

// GetRegistryEntry gets a registryentry for a user or organization
// If no such entry exists, nil is returned, both for the registryEntry and error
func (m *Manager) GetRegistryEntry(username string, globalid string, key string) (registryEntry *RegistryEntry, err error) {
	selector, err := createSelector(username, globalid, key)
	result := struct {
		Entries []RegistryEntry
	}{}
	err = m.getRegistryCollection().Find(selector).Select(bson.M{"entries.$": 1}).One(&result)
	if err == mgo.ErrNotFound {
		err = nil
		return
	}
	if result.Entries != nil && len(result.Entries) > 0 {
		registryEntry = &result.Entries[0]
	}
	return

}
