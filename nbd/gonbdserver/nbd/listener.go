package nbd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
)

// ExportConfigManager is the interface,
// that allows you to dynamically list and generate
// export configs, which have priority over
// the static configs predefined on the server
type ExportConfigManager interface {
	// List Config Names that this manager has available,
	// none can be returned in case this manager
	// does not support such a feature.
	ListConfigNames() []string
	// GetConfig returns, if possible,
	// an export Config linked to a given name
	GetConfig(name string) (*ExportConfig, error)
}

// Listener defines a single listener on a given net.Conn address
type Listener struct {
	logger          log.Logger              // a logger
	protocol        string                  // the protocol we are listening on
	addr            string                  // the address
	exports         map[string]ExportConfig // a map of static export configurations associated
	exportManager   ExportConfigManager     // an export config manager
	defaultExport   string                  // name of default export
	tls             TLSConfig               // the TLS configuration
	tlsconfig       *tls.Config             // the TLS configuration
	disableNoZeroes bool                    // disable the 'no zeroes' extension
}

// DeadlineListener defines a listener type that does what we want
type DeadlineListener interface {
	SetDeadline(t time.Time) error
	net.Listener
}

// SetExportConfigManager sets the manager used to dynamically,
// manage export configs, which has priority over the statically
// defined export configs.
func (l *Listener) SetExportConfigManager(m ExportConfigManager) {
	l.exportManager = m
}

// GetExportConfig returns a config based on a given name.
// If the ExportConfigGenerator is set and it can return a config,
// using the given name, that config will be returned.
// Otherwise it will try to find the config in the statically defined list.
// If it can't find it in that list either, or the static list is empty,
// an error will be returned.
func (l *Listener) GetExportConfig(name string) (cfg *ExportConfig, err error) {
	if l.exportManager != nil {
		// try to generate the config based on the given name,
		// generation details are defined by the implementer
		cfg, err = l.exportManager.GetConfig(name)
		if err == nil && cfg != nil {
			return
		}
		if err != nil {
			l.logger.Error(err)
		}
	}

	// try to find it in the statically defined list, if it exists.
	if l.exports != nil {
		if exportConfig, found := l.exports[name]; found {
			cfg = &exportConfig
			err = nil
			return
		}
	}

	// config could not be dynamically generated or statically found
	cfg = nil
	err = errors.Newf("no export config could be found for %q", name)
	return
}

// ListExportConfigNames returns a list of available exportNames.
// NOTE: the returned list might not be complete,
//       as it is possible that a export config manager is being used,
//       that does not support the listing of available export config names.
func (l *Listener) ListExportConfigNames() (names []string) {
	if l.exportManager != nil {
		names = l.exportManager.ListConfigNames()
	}

	// array could contain duplicates,
	// but as the dynamic exports are listed first,
	// this shouldn't give any issues.
	for name := range l.exports {
		names = append(names, name)
	}

	return
}

// Listen listens on an given address for incoming connections
//
// When sessions come in they are started on a separate context (sessionParentCtx), so that the listener can be killed without
// killing the sessions
func (l *Listener) Listen(parentCtx context.Context, sessionParentCtx context.Context, sessionWaitGroup *sync.WaitGroup) {

	addr := l.protocol + ":" + l.addr

	ctx, cancelFunc := context.WithCancel(parentCtx)

	// I know this isn't a session, but this ensures all listeners have terminated when we terminate the
	// whole thing
	sessionWaitGroup.Add(1)
	defer func() {
		cancelFunc()
		sessionWaitGroup.Done()
	}()

	if l.protocol == "unix" {
		syscall.Unlink(l.addr)
	}
	nli, err := net.Listen(l.protocol, l.addr)
	if err != nil {
		l.logger.Infof("Could not listen on address %s", addr)
		return
	}

	defer func() {
		l.logger.Infof("Stopping listening on %s", addr)
		nli.Close()
	}()

	li, ok := nli.(DeadlineListener)
	if !ok {
		l.logger.Infof("Invalid protocol to listen on %s", addr)
		return
	}

	l.logger.Infof("Starting listening on %s", addr)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		li.SetDeadline(time.Now().Add(time.Second))
		if conn, err := li.Accept(); err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			l.logger.Infof("Error %s listening on %s", err, addr)
		} else {
			l.logger.Infof("Connect to %s from %s", addr, conn.RemoteAddr())
			if connection, err := NewConnection(l, l.logger, conn); err != nil {
				l.logger.Infof("Error %s establishing connection to %s from %s", err, addr, conn.RemoteAddr())
				conn.Close()
			} else {
				go func() {
					// do not use our parent ctx as a context, as we don't want it to cancel when
					// we reload config and cancel this listener
					ctx, cancelFunc := context.WithCancel(sessionParentCtx)
					defer cancelFunc()
					sessionWaitGroup.Add(1)
					defer sessionWaitGroup.Done()
					connection.Serve(ctx)
				}()
			}
		}
	}
}

// initTLS makes an appropriate TLS config
func (l *Listener) initTLS() error {
	keyFile := l.tls.KeyFile
	if keyFile == "" {
		return nil // no TLS
	}
	certFile := l.tls.CertFile
	if certFile == "" {
		certFile = keyFile
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}

	var clientCAs *x509.CertPool
	if l.tls.CaCertFile != "" {
		clientCAs = x509.NewCertPool()
		clientCAbytes, err := ioutil.ReadFile(l.tls.CaCertFile)
		if err != nil {
			return err
		}
		if ok := clientCAs.AppendCertsFromPEM(clientCAbytes); !ok {
			return errors.New("Could not append CA certficates from PEM file")
		}
	}

	serverName := l.tls.ServerName
	if serverName == "" {
		serverName, err = os.Hostname()
		if err != nil {
			return err
		}
	}
	var minVersion uint16
	var maxVersion uint16
	var ok bool
	if l.tls.MinVersion != "" {
		minVersion, ok = tlsVersionMap[strings.ToLower(l.tls.MinVersion)]
		if !ok {
			return errors.Newf("Bad minimum TLS version: '%s'", l.tls.MinVersion)
		}
	}
	if l.tls.MaxVersion != "" {
		minVersion, ok = tlsVersionMap[strings.ToLower(l.tls.MaxVersion)]
		if !ok {
			return errors.Newf("Bad maximum TLS version: '%s'", l.tls.MaxVersion)
		}
	}

	var clientAuth tls.ClientAuthType
	if l.tls.ClientAuth != "" {
		clientAuth, ok = tlsClientAuthMap[strings.ToLower(l.tls.ClientAuth)]
		if !ok {
			return errors.Newf("Bad TLS client auth type: '%s'", l.tls.ClientAuth)
		}
	}

	l.tlsconfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   serverName,
		ClientAuth:   clientAuth,
		ClientCAs:    clientCAs,
		MinVersion:   minVersion,
		MaxVersion:   maxVersion,
	}
	return nil
}

// NewListener returns a new listener object
func NewListener(logger log.Logger, s ServerConfig) (*Listener, error) {
	if logger == nil {
		return nil, errors.New("NewListener: requires a non-nil logger")
	}

	exportMap := make(map[string]ExportConfig, len(s.Exports))
	for _, cfg := range s.Exports {
		exportMap[cfg.Name] = cfg
	}

	l := &Listener{
		logger:          logger,
		protocol:        s.Protocol,
		addr:            s.Address,
		exports:         exportMap,
		defaultExport:   s.DefaultExport,
		disableNoZeroes: s.DisableNoZeroes,
		tls:             s.TLS,
	}
	if err := l.initTLS(); err != nil {
		return nil, err
	}
	return l, nil
}
