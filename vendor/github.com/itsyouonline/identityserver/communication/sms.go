package communication

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	log "github.com/Sirupsen/logrus"
)

//SMSService defines an sms communication channel
type SMSService interface {
	Send(phonenumber string, message string) (err error)
}

//TwilioSMSService is an SMS communication channel using Twilio
type TwilioSMSService struct {
	AccountSID          string
	AuthToken           string
	MessagingServiceSID string
}

//Send sends an SMS
func (s *TwilioSMSService) Send(phonenumber string, message string) (err error) {
	client := &http.Client{}

	data := url.Values{
		"MessagingServiceSid": {s.MessagingServiceSID},
		"To":   {phonenumber},
		"Body": {message},
	}

	req, err := http.NewRequest("POST", "https://api.twilio.com/2010-04-01/Accounts/"+s.AccountSID+"/Messages.json", strings.NewReader(data.Encode()))
	if err != nil {
		log.Error("Error creating sms request: ", err)
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(s.AccountSID, s.AuthToken)
	resp, err := client.Do(req)
	if err != nil {
		log.Error("Error sending sms via Twilio: ", err)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		log.Error("Problem when sending sms via Twilio: ", resp.StatusCode, "\n", string(body))
		err = errors.New("Error sending sms")
	}
	log.Infof("SMS: sms send to %s", phonenumber)
	return
}
