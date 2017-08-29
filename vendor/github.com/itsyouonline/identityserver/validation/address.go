package validation

import (
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/itsyouonline/identityserver/db/user"
	"github.com/itsyouonline/identityserver/db/validation"
)

// MailService is the interface for a mail communication channel, should be used by the IYOAddressValidationService
type MailService interface {
	Send(address user.Address, message string) (err error)
}

// IYOAddressValidationService is the itsyou.online implementation of an AddressValidationService
type IYOAddressValidationService struct {
	MailService MailService
}

//RequestValidation validates the email address by sending an email
func (service *IYOAddressValidationService) RequestValidation(request *http.Request, username string, address user.Address, confirmationurl string, langKey string) (key string, err error) {
	valMngr := validation.NewManager(request)
	info, err := valMngr.NewAddressValidationInformation(username, address)
	if err != nil {
		log.Error(err)
		return
	}
	err = valMngr.SaveAddressValidationInformation(info)
	if err != nil {
		log.Error(err)
		return
	}

	// TODO: generate or build pdf here

	// translationFile, err := tools.LoadTranslations(langKey)
	// if err != nil {
	// 	log.Error("Error while loading translations: ", err)
	// 	return
	// }
	//
	// translations := struct {
	// 	Emailvalidation translations
	// }{}
	//
	// r := bytes.NewReader(translationFile)
	// if err = json.NewDecoder(r).Decode(&translations); err != nil {
	// 	log.Error("Error while decoding translations: ", err)
	// 	return
	// }
	//
	// validationurl := fmt.Sprintf("%s?c=%s&k=%s&l=%s", confirmationurl, url.QueryEscape(info.Secret), url.QueryEscape(info.Key), langKey)
	// templateParameters := EmailWithButtonTemplateParams{
	// 	UrlCaption: translations.Emailvalidation.Urlcaption,
	// 	Url:        validationurl,
	// 	Username:   username,
	// 	Title:      translations.Emailvalidation.Title,
	// 	Text:       fmt.Sprintf(translations.Emailvalidation.Text, email),
	// 	ButtonText: translations.Emailvalidation.Buttontext,
	// 	Reason:     translations.Emailvalidation.Reason,
	// 	LogoUrl:    fmt.Sprintf("https://%s/assets/img/its-you-online.png", request.Host),
	// }
	// message, err := tools.RenderTemplate(emailWithButtonTemplateName, templateParameters)
	// if err != nil {
	// 	return
	// }
	message := info.Key + " " + info.Secret
	go service.MailService.Send(address, message)
	key = info.Key
	return
}

//ExpireValidation removes a pending validation
func (service *IYOAddressValidationService) ExpireValidation(request *http.Request, key string) (err error) {
	if key == "" {
		return
	}
	valMngr := validation.NewManager(request)
	err = valMngr.RemoveAddressValidationInformation(key)
	return
}

func (service *IYOAddressValidationService) getAddressValidationInformation(request *http.Request, key string) (info *validation.AddressValidationInformation, err error) {
	if key == "" {
		return
	}
	valMngr := validation.NewManager(request)
	info, err = valMngr.GetByKeyAddressValidationInformation(key)
	return
}

//IsConfirmed checks wether a validation request is already confirmed
func (service *IYOAddressValidationService) IsConfirmed(request *http.Request, key string) (confirmed bool, err error) {
	info, err := service.getAddressValidationInformation(request, key)
	if err != nil {
		return
	}
	if info == nil {
		err = ErrInvalidOrExpiredKey
		return
	}
	confirmed = info.Confirmed
	return
}

//ConfirmValidation checks if the supplied code matches the username and key
func (service *IYOAddressValidationService) ConfirmValidation(request *http.Request, key, secret string) (err error) {
	info, err := service.getAddressValidationInformation(request, key)
	if err != nil {
		return
	}
	if info == nil {
		err = ErrInvalidOrExpiredKey
		return
	}
	if info.Secret != secret {
		err = ErrInvalidCode
		return
	}
	valMngr := validation.NewManager(request)
	p := valMngr.NewValidatedAddress(info.Username, info.Address)
	err = valMngr.SaveValidatedAddress(p)
	if err != nil {
		return
	}
	err = valMngr.UpdateAddressValidationInformation(key, true)
	if err != nil {
		return
	}
	return
}
