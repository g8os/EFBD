package company

import (
	"errors"
	"net/http"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/itsyouonline/identityserver/db"
)

const (
	COLLECTION_COMPANIES = "companies" // name of the company collection in mongodb
)

//InitModels initializes models in DB, if required.
func InitModels() {
	index := mgo.Index{
		Key:      []string{"globalid"},
		Unique:   true,
		DropDups: true,
	}

	db.EnsureIndex(COLLECTION_COMPANIES, index)
}

type CompanyManager struct {
	session    *mgo.Session
	collection *mgo.Collection
}

func getCompanyCollection(session *mgo.Session) *mgo.Collection {
	return db.GetCollection(session, COLLECTION_COMPANIES)
}

func NewCompanyManager(r *http.Request) *CompanyManager {
	session := db.GetDBSession(r)
	return &CompanyManager{
		session:    session,
		collection: getCompanyCollection(session),
	}
}

// Get company by ID.
func (cm *CompanyManager) Get(id string) (*Company, error) {
	var company Company

	objectId := bson.ObjectIdHex(id)

	if err := cm.collection.FindId(objectId).One(&company); err != nil {
		return nil, err
	}

	return &company, nil
}

//GetByName get a company by globalid.
func (cm *CompanyManager) GetByName(globalId string) (*Company, error) {
	var company Company

	err := cm.collection.Find(bson.M{"globalid": globalId}).One(&company)

	return &company, err
}

//Exists checks if a company exists.
func (cm *CompanyManager) Exists(globalId string) bool {
	count, _ := cm.collection.Find(bson.M{"globalid": globalId}).Count()

	return count != 1
}

func (c *Company) GetId() string {
	return c.ID.Hex()
}

// Create a company.
func (cm *CompanyManager) Create(company *Company) error {
	// TODO: Validation!

	company.ID = bson.NewObjectId()
	err := cm.collection.Insert(company)
	if mgo.IsDup(err) {
		return db.ErrDuplicate
	}
	return err
}

// Save a company.
func (cm *CompanyManager) Save(company *Company) error {
	// TODO: Validation!
	// TODO: save
	return errors.New("Save is not implemented for a company")
}

// Delete a company.
func (cm *CompanyManager) Delete(company *Company) error {
	//TODO: implement delete company
	return errors.New("Not implemented")
}
