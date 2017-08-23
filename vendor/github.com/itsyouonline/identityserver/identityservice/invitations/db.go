package invitations

import (
	"net/http"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/itsyouonline/identityserver/db"
)

const (
	mongoOrganizationRequestCollectionName = "join-organization-invitations"
)

//InvitationManager is used to store invitations
type InvitationManager struct {
	session    *mgo.Session
	collection *mgo.Collection
}

func getOrganizationRequestCollection(session *mgo.Session) *mgo.Collection {
	return db.GetCollection(session, mongoOrganizationRequestCollectionName)
}

//NewInvitationManager creates and initializes a new InvitationManager
func NewInvitationManager(r *http.Request) *InvitationManager {
	session := db.GetDBSession(r)
	return &InvitationManager{
		session:    session,
		collection: getOrganizationRequestCollection(session),
	}
}

// GetByUser gets all invitations for a user.
func (o *InvitationManager) GetByUser(username string) ([]JoinOrganizationInvitation, error) {
	orgRequests := []JoinOrganizationInvitation{}
	err := o.collection.Find(bson.M{"user": username}).All(&orgRequests)
	return orgRequests, err
}

// GetByEmail gets all invitations for an email address.
func (o *InvitationManager) GetByEmail(email string) ([]JoinOrganizationInvitation, error) {
	orgRequests := []JoinOrganizationInvitation{}
	err := o.collection.Find(bson.M{"emailaddress": email}).All(&orgRequests)
	return orgRequests, err
}

// GetByPhonenumber gets all invitations for a phone number.
func (o *InvitationManager) GetByPhonenumber(phonenumber string) ([]JoinOrganizationInvitation, error) {
	orgRequests := []JoinOrganizationInvitation{}
	err := o.collection.Find(bson.M{"phonenumber": phonenumber}).All(&orgRequests)
	return orgRequests, err
}

// FilterByUser gets all invitations for a user, filtered on status
func (o *InvitationManager) FilterByUser(username string, status string) ([]JoinOrganizationInvitation, error) {
	orgRequests := []JoinOrganizationInvitation{}
	if status == "" {
		return o.GetByUser(username)
	}
	qry := bson.M{
		"user":   username,
		"status": status,
	}
	err := o.collection.Find(qry).All(&orgRequests)
	return orgRequests, err
}

// FilterByEmail gets all invitations for an email address, filtered on status
func (o *InvitationManager) FilterByEmail(email string, status string) ([]JoinOrganizationInvitation, error) {
	orgRequests := []JoinOrganizationInvitation{}
	if status == "" {
		return o.GetByEmail(email)
	}
	qry := bson.M{
		"emailaddress": email,
		"status":       status,
	}
	err := o.collection.Find(qry).All(&orgRequests)
	return orgRequests, err
}

// FilterByPhonenumber gets all invitations for a phone number, filtered on status
func (o *InvitationManager) FilterByPhonenumber(phonenumber string, status string) ([]JoinOrganizationInvitation, error) {
	orgRequests := []JoinOrganizationInvitation{}
	if status == "" {
		return o.GetByPhonenumber(phonenumber)
	}
	qry := bson.M{
		"phonenumber": phonenumber,
		"status":      status,
	}
	err := o.collection.Find(qry).All(&orgRequests)
	return orgRequests, err
}

// GetByOrganization gets all invitations for an organization
func (o *InvitationManager) GetByOrganization(globalid string) ([]JoinOrganizationInvitation, error) {
	orgRequests := []JoinOrganizationInvitation{}
	err := o.collection.Find(bson.M{"organization": globalid}).All(&orgRequests)
	return orgRequests, err
}

// FilterByOrganization gets all invitations for a user, filtered on status
func (o *InvitationManager) FilterByOrganization(globalID string, status string) ([]JoinOrganizationInvitation, error) {
	orgRequests := []JoinOrganizationInvitation{}
	if status == "" {
		return o.GetByOrganization(globalID)
	}
	qry := bson.M{
		"organization": globalID,
		"status":       status,
	}
	err := o.collection.Find(qry).All(&orgRequests)
	return orgRequests, err
}

//Get get an invitation by it's content, not really this usefull, TODO: just make an exists method
func (o *InvitationManager) Get(username string, organization string, role string, status InvitationStatus) (*JoinOrganizationInvitation, error) {
	var orgRequest JoinOrganizationInvitation

	query := bson.M{
		"user":         username,
		"role":         role,
		"organization": organization,
		"status":       status,
	}

	err := o.collection.Find(query).One(&orgRequest)
	if err == mgo.ErrNotFound {
		return nil, nil
	}

	return &orgRequest, err
}

//GetWithEmail get an invitation by it's content, not really this usefull, TODO: just make an exists method
func (o *InvitationManager) GetWithEmail(email string, organization string, role string, status InvitationStatus) (*JoinOrganizationInvitation, error) {
	var orgRequest JoinOrganizationInvitation

	query := bson.M{
		"emailaddress": email,
		"role":         role,
		"organization": organization,
		"status":       status,
	}

	err := o.collection.Find(query).One(&orgRequest)
	if err == mgo.ErrNotFound {
		return nil, nil
	}

	return &orgRequest, err
}

//GetWithPhonenumber get an invitation by it's content, not really this usefull, TODO: just make an exists method
func (o *InvitationManager) GetWithPhonenumber(phonenumber string, organization string, role string, status InvitationStatus) (*JoinOrganizationInvitation, error) {
	var orgRequest JoinOrganizationInvitation

	query := bson.M{
		"phonenumber":  phonenumber,
		"role":         role,
		"organization": organization,
		"status":       status,
	}

	err := o.collection.Find(query).One(&orgRequest)
	if err == mgo.ErrNotFound {
		return nil, nil
	}

	return &orgRequest, err
}

// Save saves/updates an invitation
func (o *InvitationManager) Save(invite *JoinOrganizationInvitation) error {

	_, err := o.collection.Upsert(
		bson.M{
			"user":         invite.User,
			"organization": invite.Organization,
			"role":         invite.Role,
			"emailaddress": invite.EmailAddress,
			"phonenumber":  invite.PhoneNumber,
		}, invite)

	return err
}

// RemoveAll Removes all invitations linked to an organization
func (o *InvitationManager) RemoveAll(globalid string) error {
	_, err := o.collection.RemoveAll(bson.M{"organization": globalid})
	return err
}

// HasInvite Checks if a user has an invite for an organization
func (o *InvitationManager) HasInvite(globalid string, username string) (hasInvite bool, err error) {
	count, err := o.collection.Find(bson.M{"organization": globalid, "user": username}).Count()
	return count != 0, err
}

// HasPhoneInvite Checks if a phonenumber has an invite to an organization
func (o *InvitationManager) HasPhoneInvite(globalid string, phonenumber string) (hasInvite bool, err error) {
	count, err := o.collection.Find(bson.M{"organization": globalid, "phonenumber": phonenumber}).Count()
	return count != 0, err
}

// HasEmailInvite Checks if an emailaddress has an invite to an organization
func (o *InvitationManager) HasEmailInvite(globalid string, email string) (hasInvite bool, err error) {
	count, err := o.collection.Find(bson.M{"organization": globalid, "emailaddress": email}).Count()
	return count != 0, err
}

// CountByOrganization Counts the amount of invitations, filtered by an organization
func (o *InvitationManager) CountByOrganization(globalid string) (int, error) {
	count, err := o.collection.Find(bson.M{"organization": globalid}).Count()
	return count, err
}

// GetByCode Gets an invite by code
func (o *InvitationManager) GetByCode(code string) (invite *JoinOrganizationInvitation, err error) {
	qry := bson.M{
		"code": code,
	}
	err = o.collection.Find(qry).One(&invite)
	return
}

// SetAcceptedByCode Sets an invite as "accepted"
func (o *InvitationManager) SetAcceptedByCode(code string) error {
	qry := bson.M{
		"code": code,
	}
	update := bson.M{
		"$set": bson.M{
			"status": RequestAccepted,
		},
	}
	return o.collection.Update(qry, update)
}

// Remove removes an invitation
func (o *InvitationManager) Remove(globalID string, searchString string) error {
	qry := bson.M{
		"organization": globalID,
		"$or": []bson.M{
			{"user": searchString},
			{"emailaddress": searchString},
			{"phonenumber": searchString},
		},
	}
	return o.collection.Remove(qry)
}

// GetInvites gets all invites where the organization is invited
func (o *InvitationManager) GetOpenOrganizationInvites(globalID string) ([]JoinOrganizationInvitation, error) {
	invites := []JoinOrganizationInvitation{}
	qry := bson.M{
		"user":   globalID,
		"status": "pending",
		//"isorganization": true,
	}
	err := o.collection.Find(qry).All(&invites)
	if err == mgo.ErrNotFound {
		err = nil
	}
	return invites, err
}
