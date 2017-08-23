package communication

import log "github.com/Sirupsen/logrus"

//DevSMSService is a fake sms service that just logs the sms that should be send
type DevSMSService struct {
}

//Send sends an SMS
func (s *DevSMSService) Send(phonenumber string, message string) (err error) {
	log.Infof("SMS: In production an sms would be sent to %s with the following content:\n%s", phonenumber, message)
	return
}
