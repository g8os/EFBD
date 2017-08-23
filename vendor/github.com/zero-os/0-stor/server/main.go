package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	units "github.com/docker/go-units"
	"github.com/zero-os/0-stor/server/config"
	"github.com/zero-os/0-stor/server/db/badger"
	"github.com/zero-os/0-stor/server/disk"
	"github.com/zero-os/0-stor/server/goraml"
	"github.com/zero-os/0-stor/server/manager"
	"github.com/zero-os/0-stor/server/storserver"

	"gopkg.in/validator.v2"
)

const version = "0.0.1"

func main() {
	app := cli.NewApp()
	app.Name = "zerostorserver"
	app.Usage = "Generic object store used by zero-os"
	app.Description = "Generic object store used by zero-os"
	app.Version = version

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetOutput(os.Stdout)
	settings := config.Settings{}
	var profileAddr string

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:        "debug, d",
			Usage:       "Enable debug logging",
			Destination: &settings.DebugLog,
		},
		cli.StringFlag{
			Name:        "bind, b",
			Usage:       "Bind address",
			Value:       ":8080",
			Destination: &settings.BindAddress,
		},
		cli.StringFlag{
			Name:        "data",
			Usage:       "Data directory",
			Value:       ".db/data",
			Destination: &settings.DB.Dirs.Data,
		},
		cli.StringFlag{
			Name:        "meta",
			Usage:       "Metadata directory",
			Value:       ".db/meta",
			Destination: &settings.DB.Dirs.Meta,
		},
		cli.StringFlag{
			Name:        "interface",
			Usage:       "type of server, can be rest or grpc",
			Value:       "rest",
			Destination: &settings.ServerType,
		},
		cli.StringFlag{
			Name:        "profile-addr",
			Usage:       "Enables profiling of this server as an http service",
			Value:       "",
			Destination: &profileAddr,
		},
	}

	app.Before = func(c *cli.Context) error {
		if settings.DebugLog {
			log.SetLevel(log.DebugLevel)
			log.Debug("Debug logging enabled")
		}
		// input validator
		validator.SetValidationFunc("multipleOf", goraml.MultipleOf)

		if settings.ServerType != config.ServerTypeRest && settings.ServerType != config.ServerTypeGrpc {
			log.Fatalf("%s is not a supported server interface\n", settings.ServerType)
		}

		return nil
	}

	app.Action = func(c *cli.Context) {
		log.Infoln(app.Name, "version", app.Version)

		if err := ensureStoreStat(settings.DB.Dirs.Data, settings.DB.Dirs.Meta); err != nil {
			log.Fatalln("Error computing store statistics: %v", err)
		}

		var (
			server storserver.StoreServer
			err    error
		)

		switch settings.ServerType {
		case config.ServerTypeRest:
			server, err = storserver.NewRest(settings.DB.Dirs.Data, settings.DB.Dirs.Meta)
			if err != nil {
				log.Fatal(err.Error())
			}
		case config.ServerTypeGrpc:
			server, err = storserver.NewGRPC(settings.DB.Dirs.Data, settings.DB.Dirs.Meta)
			if err != nil {
				log.Fatal(err.Error())
			}
		default:
			log.Fatalf("%s is not a supported server interface\n", settings.ServerType)
		}

		if profileAddr != "" {
			go func() {
				log.Infof("profiling enabled on %v", profileAddr)
				if err := http.ListenAndServe(profileAddr, http.DefaultServeMux); err != nil {
					log.Infof("Failed to enable profiling on %v, err:%v", profileAddr, err)
				}
			}()
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT)

		addr, err := server.Listen(settings.BindAddress)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Infof("Server interface: %s", settings.ServerType)
		log.Infof("Server listening on %s", addr)

		<-sigChan // block on signal handler
		log.Println("Gracefully closing 0-stor")
		server.Close()

		os.Exit(0)
	}

	app.Run(os.Args)
}

func ensureStoreStat(dataPath, metaPath string) error {
	kv, err := badger.New(dataPath, metaPath)
	if err != nil {
		return err
	}
	defer kv.Close()

	nsMgr := manager.NewNamespaceManager(kv)
	statMgr := manager.NewStoreStatMgr(kv)

	available, err := disk.FreeSpace(dataPath)
	if err != nil {
		return err
	}

	namespaces, err := kv.List([]byte(manager.NAMESPACE_PREFIX))
	if err != nil {
		return err
	}

	var totalReserved uint64
	for _, namespace := range namespaces {
		ns, err := nsMgr.Get(string(namespace))
		if err != nil {
			return err
		}
		totalReserved += ns.Reserved
	}

	available = available - totalReserved

	if available <= 0 {
		return fmt.Errorf("total reserved size exceed availale disk space")
	}

	if err := statMgr.Set(available, totalReserved); err != nil {
		return err
	}

	log.Infof("Space reserved : %s", units.BytesSize(float64(totalReserved)))
	log.Infof("Space available : %s", units.BytesSize(float64(available)))

	return nil
}
