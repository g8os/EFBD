package tools

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"html/template"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/itsyouonline/identityserver/templates/packaged"
)

const (
	defaultTranslations = "i18n/en.json"
)

func GenerateRandomString() (randomString string, err error) {
	b := make([]byte, 32)
	_, err = rand.Read(b)
	if err != nil {
		return
	}
	// Use URLencoding to avoid '/' characters. The generated string it then safe to use
	// in URLs
	randomString = base64.URLEncoding.EncodeToString(b)
	return
}

func RenderTemplate(templateName string, data interface{}) (message string, err error) {
	log.Debug("Email template: ", templateName)
	log.Debug("Email Data: ", data)
	htmlData, err := templates.Asset(templateName)
	if err != nil {
		log.Error("Could not get email asset: ", err)
		return
	}
	templateEngine := template.New("template")
	templateEngine, err = templateEngine.Parse(string(htmlData))
	if err != nil {
		log.Error("Could parse template: ", err)
		return
	}
	buf := new(bytes.Buffer)
	templateEngine.Execute(buf, data)
	message = buf.String()
	return
}

// LoadTranslations tries to load the translation file for a given language key. If the file does not exist, or can't be opened, the default translations (english) will be loaded.
func LoadTranslations(rawKey string) (translationFile []byte, err error) {
	langKey := parseLangKey(rawKey)
	assetName := "i18n/" + langKey + ".json"
	translationFile, err = templates.Asset(assetName)
	// translation file doesn't exis, or there is an error loading it
	if err != nil {
		// try and use the default translations
		translationFile, err = templates.Asset(defaultTranslations)
		if err != nil {
			log.Error("Error while loading translations: ", err)
		}
	}
	return
}

// parseLangKey return the first 2 characters of a string in lowercase. If the string is empty or has only 1 character, and empty string is returned
func parseLangKey(rawKey string) string {
	if len(rawKey) < 2 {
		return ""
	}
	return strings.ToLower(string(rawKey[0:2]))
}
