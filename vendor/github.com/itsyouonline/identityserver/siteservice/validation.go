package siteservice

import (
	"bytes"
	"encoding/json"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/itsyouonline/identityserver/tools"
	"github.com/itsyouonline/identityserver/validation"
)

//PhonenumberValidation is the page that is linked to in the SMS for phonenumbervalidation and is thus accessed on the mobile phone
func (service *Service) PhonenumberValidation(w http.ResponseWriter, request *http.Request) {

	err := request.ParseForm()
	if err != nil {
		log.Debug(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	values := request.Form
	key := values.Get("k")
	smscode := values.Get("c")
	langKey := values.Get("l")

	translationFile, err := tools.LoadTranslations(langKey)
	if err != nil {
		log.Error("Error while loading translations: ", err)
		return
	}

	translations := struct {
		Invalidlink  string
		Error        string
		Smsconfirmed string
	}{}

	r := bytes.NewReader(translationFile)
	if err = json.NewDecoder(r).Decode(&translations); err != nil {
		log.Error("Error while decoding translations: ", err)
		return
	}

	err = service.phonenumberValidationService.ConfirmValidation(request, key, smscode)
	if err == validation.ErrInvalidCode || err == validation.ErrInvalidOrExpiredKey {
		service.renderSMSConfirmationPage(w, request, translations.Invalidlink)
		return
	}
	if err != nil {
		log.Error(err)
		service.renderSMSConfirmationPage(w, request, translations.Error)
		return
	}

	service.renderSMSConfirmationPage(w, request, translations.Smsconfirmed)
}

func (service *Service) EmailValidation(w http.ResponseWriter, request *http.Request) {

	err := request.ParseForm()
	if err != nil {
		log.Debug(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	values := request.Form
	key := values.Get("k")
	smscode := values.Get("c")
	langKey := values.Get("l")

	translationFile, err := tools.LoadTranslations(langKey)
	if err != nil {
		log.Error("Error while loading translations: ", err)
		return
	}

	translations := struct {
		Invalidlink    string
		Error          string
		Emailconfirmed string
	}{}

	r := bytes.NewReader(translationFile)
	if err = json.NewDecoder(r).Decode(&translations); err != nil {
		log.Error("Error while decoding translations: ", err)
		return
	}

	err = service.emailaddressValidationService.ConfirmValidation(request, key, smscode)
	if err == validation.ErrInvalidCode || err == validation.ErrInvalidOrExpiredKey {
		service.renderEmailConfirmationPage(w, request, translations.Invalidlink)
		return
	}
	if err != nil {
		log.Error(err)
		service.renderEmailConfirmationPage(w, request, translations.Error)
		return
	}

	service.renderEmailConfirmationPage(w, request, translations.Emailconfirmed)
}
