package https

import (
	"os"

	"crypto/tls"
	"io/ioutil"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

func PrepareHTTP(bindAddress string, handler http.Handler) *http.Server {
	s := &http.Server{
		Addr:    bindAddress,
		Handler: handler,
	}

	return s
}

func PrepareHTTPS(s *http.Server, cert string, key string, skipDev bool) error {
	// Checking for TLS default keys
	var certBytes, keyBytes []byte

	devCert := "devcert/cert.pem"
	devKey := "devcert/key.pem"

	// If only a single argument is set
	if (cert == "" || key == "") && (cert != "" || key != "") {
		log.Panic("You cannot specify only key or certificate")
	}

	// Using default certificate
	if cert == "" && !skipDev {
		if _, err := os.Stat(devCert); err == nil {
			if _, err := os.Stat(devKey); err == nil {
				log.Warning("===============================================================================")
				log.Warning("This instance will use development certificates, don't use them in production !")
				log.Warning("===============================================================================")

				cert = devCert
				key = devKey

			} else {
				log.Debug("No devcert key found: ", devKey)
			}

		} else {
			log.Debug("No devcert certificate found: ", devCert)
		}
	}

	if _, err := os.Stat(cert); err != nil {
		if _, err := os.Stat(key); err != nil {
			certBytes, keyBytes = GenerateDefaultTLS(cert, key)
		}

	} else {
		log.Info("Loading TLS Certificate: ", cert)
		log.Info("Loading TLS Private key: ", key)

		certBytes, err = ioutil.ReadFile(cert)
		keyBytes, err = ioutil.ReadFile(key)
	}

	certifs, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		log.Panic("Cannot parse certificates")
	}

	s.TLSConfig = &tls.Config{
		Certificates: []tls.Certificate{certifs},
	}

	return nil
}
