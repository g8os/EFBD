package siteservice

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/sessions"
	"github.com/itsyouonline/identityserver/credentials/password"
	"github.com/itsyouonline/identityserver/db"
	"github.com/itsyouonline/identityserver/db/organization"
	"github.com/itsyouonline/identityserver/db/user"
	"github.com/itsyouonline/identityserver/siteservice/website/packaged/html"
	"github.com/itsyouonline/identityserver/validation"
)

const (
	mongoRegistrationCollectionName = "registrationsessions"
	MAX_PENDING_REGISTRATION_COUNT  = 10000
)

//initLoginModels initialize models in mongo
func (service *Service) initRegistrationModels() {
	index := mgo.Index{
		Key:      []string{"sessionkey"},
		Unique:   true,
		DropDups: false,
	}

	db.EnsureIndex(mongoRegistrationCollectionName, index)

	automaticExpiration := mgo.Index{
		Key:         []string{"createdat"},
		ExpireAfter: time.Second * 60 * 10, //10 minutes
		Background:  true,
	}
	db.EnsureIndex(mongoRegistrationCollectionName, automaticExpiration)

}

type registrationSessionInformation struct {
	SessionKey           string
	SMSCode              string
	Confirmed            bool
	ConfirmationAttempts uint
	CreatedAt            time.Time
}

const (
	registrationFileName = "registration.html"
)

func (service *Service) renderRegistrationFrom(w http.ResponseWriter, request *http.Request) {
	htmlData, err := html.Asset(registrationFileName)
	if err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	sessions.Save(request, w)
	w.Write(htmlData)
}

//CheckRegistrationSMSConfirmation is called by the sms code form to check if the sms is already confirmed on the mobile phone
func (service *Service) CheckRegistrationSMSConfirmation(w http.ResponseWriter, request *http.Request) {
	registrationSession, err := service.GetSession(request, SessionForRegistration, "registrationdetails")
	if err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	response := map[string]bool{}

	if registrationSession.IsNew {
		// TODO: registrationSession is new with SMS, something must be wrong
		log.Warn("Registration is new")
		response["confirmed"] = true //This way the form will be submitted, let the form handler deal with redirect to login
	} else {
		validationkey, _ := registrationSession.Values["phonenumbervalidationkey"].(string)

		confirmed, err := service.phonenumberValidationService.IsConfirmed(request, validationkey)
		if err == validation.ErrInvalidOrExpiredKey {
			confirmed = true //This way the form will be submitted, let the form handler deal with redirect to login
			return
		}
		if err != nil {
			log.Error(err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		response["confirmed"] = confirmed
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

//CheckRegistrationEmailConfirmation is called by the regisration form to check if the email is already confirmed
func (service *Service) CheckRegistrationEmailConfirmation(w http.ResponseWriter, r *http.Request) {
	registrationSession, err := service.GetSession(r, SessionForRegistration, "registrationdetails")
	if err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	response := map[string]bool{}

	if registrationSession.IsNew {
		// TODO: registrationSession is new, something must be wrong
		log.Warn("Registration is new")
		response["confirmed"] = true //This way the form will be submitted, let the form handler deal with redirect to login
	} else {
		validationkey, _ := registrationSession.Values["emailvalidationkey"].(string)

		confirmed, err := service.emailaddressValidationService.IsConfirmed(r, validationkey)
		if err == validation.ErrInvalidOrExpiredKey {
			// TODO
			confirmed = true //This way the form will be submitted, let the form handler deal with redirect to login
			return
		}
		if err != nil {
			log.Error(err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		response["confirmed"] = confirmed
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

//ShowRegistrationForm shows the user registration page
func (service *Service) ShowRegistrationForm(w http.ResponseWriter, request *http.Request) {
	service.renderRegistrationFrom(w, request)
}

//ProcessPhonenumberConfirmationForm processes the Phone number confirmation form
func (service *Service) ProcessPhonenumberConfirmationForm(w http.ResponseWriter, request *http.Request) {
	values := struct {
		Smscode string `json:"smscode"`
	}{}

	response := struct {
		RedirectUrL string `json:"redirecturl"`
		Error       string `json:"error"`
	}{}

	if err := json.NewDecoder(request.Body).Decode(&values); err != nil {
		log.Debug("Error decoding the ProcessPhonenumberConfirmation request:", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	registrationSession, err := service.GetSession(request, SessionForRegistration, "registrationdetails")
	if err != nil {
		log.Debug(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if registrationSession.IsNew {
		sessions.Save(request, w)
		response.RedirectUrL = fmt.Sprintf("https://%s/register/#/smsconfirmation", request.Host)
		json.NewEncoder(w).Encode(&response)
		return
	}

	username, _ := registrationSession.Values["username"].(string)
	validationkey, _ := registrationSession.Values["phonenumbervalidationkey"].(string)

	if isConfirmed, _ := service.phonenumberValidationService.IsConfirmed(request, validationkey); isConfirmed {
		userMgr := user.NewManager(request)
		userMgr.RemoveExpireDate(username)
		service.loginUser(w, request, username)
		return
	}

	smscode := values.Smscode
	if err != nil || smscode == "" {
		log.Debug(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	err = service.phonenumberValidationService.ConfirmValidation(request, validationkey, smscode)
	if err == validation.ErrInvalidCode {
		w.WriteHeader(http.StatusUnprocessableEntity)
		response.Error = "invalid_sms_code"
		json.NewEncoder(w).Encode(&response)
		return
	}
	if err == validation.ErrInvalidOrExpiredKey {
		sessions.Save(request, w)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		json.NewEncoder(w).Encode(&response)
		return
	} else if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	userMgr := user.NewManager(request)
	userMgr.RemoveExpireDate(username)
	service.loginUser(w, request, username)
}

//ResendPhonenumberConfirmation resend the phonenumberconfirmation to a possbily new phonenumber
func (service *Service) ResendPhonenumberConfirmation(w http.ResponseWriter, request *http.Request) {
	values := struct {
		PhoneNumber string `json:"phonenumber"`
		LangKey     string `json:"langkey"`
	}{}

	response := struct {
		RedirectUrL string `json:"redirecturl"`
		Error       string `json:"error"`
	}{}

	if err := json.NewDecoder(request.Body).Decode(&values); err != nil {
		log.Debug("Error decoding the ResendPhonenumberConfirmation request: ", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	registrationSession, err := service.GetSession(request, SessionForRegistration, "registrationdetails")
	if err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if registrationSession.IsNew {
		sessions.Save(request, w)
		log.Debug("Registration session expired")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	username, _ := registrationSession.Values["username"].(string)

	//Invalidate the previous validation request, ignore a possible error
	validationkey, _ := registrationSession.Values["phonenumbervalidationkey"].(string)
	_ = service.phonenumberValidationService.ExpireValidation(request, validationkey)

	phonenumber := user.Phonenumber{Label: "main", Phonenumber: values.PhoneNumber}
	if !phonenumber.Validate() {
		log.Debug("Invalid phone number")
		w.WriteHeader(http.StatusUnprocessableEntity)
		response.Error = "invalid_phonenumber"
		json.NewEncoder(w).Encode(&response)
		return
	}

	uMgr := user.NewManager(request)
	err = uMgr.SavePhone(username, phonenumber)
	if err != nil {
		log.Error("ResendPhonenumberConfirmation: Could not save phonenumber: ", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	validationkey, err = service.phonenumberValidationService.RequestValidation(request, username, phonenumber, fmt.Sprintf("https://%s/phonevalidation", request.Host), values.LangKey)
	if err != nil {
		log.Error("ResendPhonenumberConfirmation: Could not get validationkey: ", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	registrationSession.Values["phonenumbervalidationkey"] = validationkey

	sessions.Save(request, w)
	response.RedirectUrL = fmt.Sprintf("https://%s/register/#smsconfirmation", request.Host)
	json.NewEncoder(w).Encode(&response)
}

//ProcessRegistrationForm processes the user registration form
func (service *Service) ProcessRegistrationForm(w http.ResponseWriter, r *http.Request) {
	response := struct {
		Redirecturl string `json:"redirecturl"`
		Error       string `json:"error"`
	}{}
	values := struct {
		Firstname       string `json:"firstname"`
		Lastname        string `json:"lastname"`
		Email           string `json:"email"`
		EmailCode       string `json:"emailcode"`
		Phonenumber     string `json:"phonenumber"`
		PhonenumberCode string `json:"phonenumbercode"`
		Password        string `json:"password"`
		RedirectParams  string `json:"redirectparams"`
		LangKey         string `json:"langkey"`
	}{}
	if err := json.NewDecoder(r.Body).Decode(&values); err != nil {
		log.Debug("Error decoding the registration request:", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	registrationSession, err := service.GetSession(r, SessionForRegistration, "registrationdetails")
	if err != nil {
		log.Error("Failed to retrieve registration session: ", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if registrationSession.IsNew {
		sessions.Save(r, w)
		log.Debug("Registration session expired")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	username, _ := registrationSession.Values["username"].(string)

	userMgr := user.NewManager(r)

	// check if phone number is validated or sms code is provided to validate phone
	phonevalidationkey, _ := registrationSession.Values["phonenumbervalidationkey"].(string)

	if isConfirmed, _ := service.phonenumberValidationService.IsConfirmed(r, phonevalidationkey); !isConfirmed {

		smscode := values.PhonenumberCode
		if smscode == "" {
			log.Debug("no sms code provided and phone not confirmed yet")
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		err = service.phonenumberValidationService.ConfirmValidation(r, phonevalidationkey, smscode)
		if err == validation.ErrInvalidCode {
			w.WriteHeader(http.StatusUnprocessableEntity)
			response.Error = "invalid_sms_code"
			json.NewEncoder(w).Encode(&response)
			return
		}
		if err == validation.ErrInvalidOrExpiredKey {
			sessions.Save(r, w)
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			json.NewEncoder(w).Encode(&response)
			return
		}
		if err != nil {
			log.Error("Error while trying to validate phone number in regsitration flow: ", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	}
	// at this point the phone number is confirmed
	userMgr.RemoveExpireDate(username)
	// see if we can also verify the email, and if we can't, see if we can continue the registration

	// require a validated email to register if:
	//  - a validated email scope is required to log in to an external org
	//  - the user is registering against IYO (not in an oauth flow)
	//  - the `requirevalidatedemail` queryparameter is set.

	requireValidatedEmail := false
	queryParams, err := url.ParseQuery(values.RedirectParams)
	if err != nil {
		log.Debug("Failed to parse query params: ", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	if strings.Contains(queryParams.Get("scope"), "user:validated:email") {
		log.Debug("Require validated email because of user:validated:email scope")
		requireValidatedEmail = true
	}
	if queryParams.Get("client_id") == "" {
		log.Debug("Require validated email because there is no client id")
		requireValidatedEmail = true
	}
	if queryParams.Get("requirevalidatedemail") != "" {
		log.Debug("Require validated email because the requirevalidatedemail query parameter is set")
		requireValidatedEmail = true
	}

	emailConfirmed := false
	emailvalidationkey, _ := registrationSession.Values["emailvalidationkey"].(string)
	if isConfirmed, _ := service.emailaddressValidationService.IsConfirmed(r, emailvalidationkey); isConfirmed {
		emailConfirmed = true
	}
	if !emailConfirmed {
		emailCode := values.EmailCode
		if emailCode == "" && requireValidatedEmail {
			log.Debug("no email code provided and email not confirmed yet")
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		err = service.emailaddressValidationService.ConfirmValidation(r, emailvalidationkey, emailCode)
		if err == validation.ErrInvalidCode {
			w.WriteHeader(http.StatusUnprocessableEntity)
			response.Error = "invalid_email_code"
			json.NewEncoder(w).Encode(&response)
			return
		}
		if err == validation.ErrInvalidOrExpiredKey {
			sessions.Save(r, w)
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			json.NewEncoder(w).Encode(&response)
			return
		}
		if err != nil {
			log.Error("Error while trying to validate email address in regsitration flow: ", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	}

	registrationSession.Values["redirectparams"] = values.RedirectParams

	sessions.Save(r, w)
	service.loginUser(w, r, username)
}

//ValidateUsername checks if a username is already taken or not
func (service *Service) ValidateUsername(w http.ResponseWriter, request *http.Request) {
	username := request.URL.Query().Get("username")
	response := struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}{
		Valid: true,
		Error: "",
	}
	valid := user.ValidateUsername(username)
	if !valid {
		log.Debug("Invalid username format:", username)
		response.Error = "invalid_username_format"
		response.Valid = false
		json.NewEncoder(w).Encode(&response)
		return
	}
	userMgr := user.NewManager(request)
	orgMgr := organization.NewManager(request)
	userExists, err := userMgr.Exists(username)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if userExists {
		log.Debug("username ", username, " already taken")
		response.Error = "user_exists"
		response.Valid = false
	} else {
		orgExists := orgMgr.Exists(username)
		if orgExists {
			log.Debugf("Organization with name %s already exists", username)
			response.Error = "organization_exists"
			response.Valid = false
		}
	}
	json.NewEncoder(w).Encode(&response)
	return
}

// Starts validation for a temporary username
func (service *Service) ValidateInfo(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Firstname string `json:"firstname"`
		Lastname  string `json:"lastname"`
		Email     string `json:"email"`
		Phone     string `json:"phone"`
		Password  string `json:"password"`
		LangKey   string `json:"langkey"`
	}{}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		log.Debug("Failed to decode validate info body: ", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	counter := 0
	username := strings.ToLower(data.Firstname) + "_" + strings.ToLower(data.Lastname) + "_"
	userMgr := user.NewManager(r)

	count, err := userMgr.GetPendingRegistrationsCount()
	if err != nil {
		log.Error("Failed to get pending registerations count: ", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	log.Debug("count", count)
	if count >= MAX_PENDING_REGISTRATION_COUNT {
		log.Warn("Maximum amount of pending registrations reached")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	orgMgr := organization.NewManager(r)
	exists := true
	for exists {
		counter += 1
		var err error
		exists, err = userMgr.Exists(username + strconv.Itoa(counter))
		if err != nil {
			log.Error("Failed to verify if username is taken: ", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		if !exists {
			exists = orgMgr.Exists(username + strconv.Itoa(counter))
		}
	}
	username = username + strconv.Itoa(counter)
	if !user.ValidateUsername(username) {
		log.Debug("Invalid generated username: ", username)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	valid := user.ValidateEmailAddress(data.Email)
	if !valid {
		response := struct {
			Error string `json:"error"`
		}{
			Error: "invalid_email_format",
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(response)
		return
	}

	valid = user.ValidatePhoneNumber(data.Phone)
	if !valid {
		response := struct {
			Error string `json:"error"`
		}{
			Error: "invalid_phonenumber",
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(response)
		return
	}

	registrationSession, err := service.GetSession(r, SessionForRegistration, "registrationdetails")
	if err != nil {
		log.Error("Failed to retrieve registration session: ", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	validatingPhonenumber, _ := registrationSession.Values["phonenumber"].(string)
	validatingEmail, _ := registrationSession.Values["email"].(string)
	validatingUsername, _ := registrationSession.Values["username"].(string)
	validatingPassword, _ := registrationSession.Values["password"].(string)

	if validatingUsername != username || validatingEmail != data.Email || validatingPhonenumber != data.Phone {

		newuser := &user.User{
			Username:       username,
			Firstname:      data.Firstname,
			Lastname:       data.Lastname,
			EmailAddresses: []user.EmailAddress{{Label: "main", EmailAddress: data.Email}},
			Phonenumbers:   []user.Phonenumber{{Label: "main", Phonenumber: data.Phone}},
		}

		// give users a day to validate a phone number on their accounts
		duration := time.Duration(time.Hour * 24)
		expiresAt := time.Now()
		expiresAt = expiresAt.Add(duration)
		eat := db.DateTime(expiresAt)
		newuser.Expire = &eat
		userMgr.Save(newuser)
		registrationSession.Values["username"] = username

	}

	if validatingPassword != data.Password || validatingUsername != username {
		log.Debug("Saving user password")
		passwdMgr := password.NewManager(r)
		err = passwdMgr.Save(username, data.Password)
		if err != nil {
			log.Error("Error while saving the users password: ", err)
			if err.Error() != "internal_error" {
				w.WriteHeader(http.StatusUnprocessableEntity)
				response := struct {
					Error string `json:"error"`
				}{
					Error: "invalid_password",
				}
				json.NewEncoder(w).Encode(&response)
			} else {
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
			return
		}
		registrationSession.Values["password"] = data.Password
	}

	// phone number validation
	if validatingPhonenumber != data.Phone {
		phonenumber := user.Phonenumber{Phonenumber: data.Phone}
		validationkey, err := service.phonenumberValidationService.RequestValidation(r, username, phonenumber, fmt.Sprintf("https://%s/phonevalidation", r.Host), data.LangKey)
		if err != nil {
			log.Error("Failed to send phonenumber verification in registration flow: ", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		registrationSession.Values["phonenumbervalidationkey"] = validationkey
		registrationSession.Values["phonenumber"] = phonenumber.Phonenumber
	}

	if validatingEmail != data.Email {
		mailvalidationkey, err := service.emailaddressValidationService.RequestValidation(r, username, data.Email, fmt.Sprintf("https://%s/emailvalidation", r.Host), data.LangKey)
		if err != nil {
			log.Error("Failed to send email verification in registration flow: ", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		registrationSession.Values["emailvalidationkey"] = mailvalidationkey
		registrationSession.Values["email"] = data.Email
	}

	sessions.Save(r, w)
	// validations created
	w.WriteHeader(http.StatusCreated)
}

func (service *Service) ResendValidationInfo(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Email   string `json:"email"`
		Phone   string `json:"phone"`
		LangKey string `json:"langkey"`
	}{}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		log.Debug("Failed to decode validate info body: ", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	registrationSession, err := service.GetSession(r, SessionForRegistration, "registrationdetails")
	if err != nil {
		log.Error(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	if registrationSession.IsNew {
		sessions.Save(r, w)
		log.Debug("Registration session expired")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	username, _ := registrationSession.Values["username"].(string)

	//Invalidate the previous phone validation request, ignore a possible error
	validationkey, _ := registrationSession.Values["phonenumbervalidationkey"].(string)
	_ = service.phonenumberValidationService.ExpireValidation(r, validationkey)

	phonenumber, _ := registrationSession.Values["phonenumber"].(string)

	validationkey, err = service.phonenumberValidationService.RequestValidation(r, username, user.Phonenumber{Phonenumber: phonenumber}, fmt.Sprintf("https://%s/phonevalidation", r.Host), data.LangKey)
	if err != nil {
		log.Error("ResendPhonenumberConfirmation: Could not get validationkey: ", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	registrationSession.Values["phonenumbervalidationkey"] = validationkey

	//Invalidate the previous email validation request, ignore a possible error
	emailvalidationkey, _ := registrationSession.Values["emailvalidationkey"].(string)
	_ = service.emailaddressValidationService.ExpireValidation(r, emailvalidationkey)

	email, _ := registrationSession.Values["email"].(string)

	emailvalidationkey, err = service.emailaddressValidationService.RequestValidation(r, username, email, fmt.Sprintf("https://%s/emailvalidation", r.Host), data.LangKey)
	if err != nil {
		log.Error("ResendEmailConfirmation: Could not get validationkey: ", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	registrationSession.Values["emailvalidationkey"] = emailvalidationkey

	sessions.Save(r, w)
	w.WriteHeader(http.StatusOK)
}
