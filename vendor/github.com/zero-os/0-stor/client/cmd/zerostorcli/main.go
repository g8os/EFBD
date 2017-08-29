package main

import (
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli"
	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/config"
	"github.com/zero-os/0-stor/client/itsyouonline"
)

var (
	confFile string
)

func main() {
	var cl *client.Client
	var nsMgr itsyouonline.NamespaceManager

	app := cli.NewApp()
	app.Name = "0-stor cli"
	app.Usage = "Interact with 0-stors"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "conf",
			Value:       "config.yaml",
			Usage:       "path to the configuration file",
			Destination: &confFile,
		},
	}
	app.Commands = []cli.Command{
		{
			Name:  "file",
			Usage: "Command to upload/download files",
			Before: func(c *cli.Context) error {
				var err error
				cl, err = getClient(c)
				if err != nil {
					return cli.NewExitError(err, 1)
				}
				return nil
			},
			Subcommands: []cli.Command{
				{
					Name:  "upload",
					Usage: "upload a file. e.g: cli file upload myfile",
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 1 {
							return cli.NewExitError("need to give the path to the file to upload", 1)
						}

						fileName := c.Args().First()

						f, err := os.Open(fileName)
						if err != nil {
							return cli.NewExitError(fmt.Errorf("can't read the file: %v", err), 1)
						}

						if _, err := cl.WriteF([]byte(fileName), f, nil, nil, nil); err != nil {
							return cli.NewExitError(fmt.Errorf("upload failed : %v", err), 1)
						}
						fmt.Printf("file uploaded, key = %v\n", fileName)

						return nil
					},
				},
				{
					Name:  "download",
					Usage: "download a file. e.g: cli file download myfile",
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 2 {
							return cli.NewExitError(fmt.Errorf("need to give the path to the key of file to download and the destination"), 1)
						}

						key := c.Args().Get(0)
						output := c.Args().Get(1)
						fOutput, err := os.Create(output)
						if err != nil {
							return cli.NewExitError(fmt.Errorf("can't create output file: %v", err), 1)
						}

						_, err = cl.ReadF([]byte(key), fOutput)
						if err != nil {
							return cli.NewExitError(fmt.Errorf("download file failed: %v", err), 1)
						}
						fmt.Printf("file downloaded to %s\n", output)

						return nil
					},
				},
			},
		},
		{
			Name:  "namespace",
			Usage: "Manage namespaces",
			Before: func(c *cli.Context) error {
				var err error
				nsMgr, err = getNamespaceManager(c)
				if err != nil {
					return cli.NewExitError(err, 1)
				}
				return nil
			},
			Subcommands: []cli.Command{
				{
					Name:  "create",
					Usage: "Create a namespace. e.g: 'cli namespace create mynamespace'",
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 1 {
							return cli.NewExitError(fmt.Errorf("need to give the name of the namespace to create"), 1)
						}

						namespace := c.Args().First()
						if err := nsMgr.CreateNamespace(namespace); err != nil {
							return cli.NewExitError(fmt.Errorf("creation of namespace %s failed: %v", namespace, err), 1)
						}
						fmt.Printf("Namespace %s created\n", namespace)

						return nil
					},
				},

				{
					Name:  "delete",
					Usage: "Delete a namespace. e.g: 'cli namespace delete mynamespace'",
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 1 {
							return cli.NewExitError(fmt.Errorf("need to give the name of the namespace to create"), 1)
						}

						namespace := c.Args().First()
						if err := nsMgr.DeleteNamespace(namespace); err != nil {
							return cli.NewExitError(err, 1)
						}
						fmt.Printf("Namespace %s deleted\n", namespace)
						return nil
					},
				},
				{
					Name:  "set-acl",
					Usage: "Set permission to a user",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "namespace",
							Usage: "Label of the namespace",
						},
						cli.StringFlag{
							Name:  "user",
							Usage: "ItsYouOnline user id",
						},
						cli.BoolFlag{
							Name:  "read, r",
							Usage: "set read permission",
						},
						cli.BoolFlag{
							Name:  "write, w",
							Usage: "set write permission",
						},
						cli.BoolFlag{
							Name:  "delete, d",
							Usage: "set delete permission",
						},
						cli.BoolFlag{
							Name:  "admin, a",
							Usage: "set admin permission",
						},
					},
					Action: func(c *cli.Context) error {
						namespace := c.String("namespace")
						user := c.String("user")
						currentPermision, err := nsMgr.GetPermission(namespace, user)
						if err != nil {
							return cli.NewExitError(fmt.Errorf("fail to retrieve permission : %v", err), 1)
						}

						requestedPermission := itsyouonline.Permission{
							Read:   c.Bool("r"),
							Write:  c.Bool("w"),
							Delete: c.Bool("d"),
							Admin:  c.Bool("a"),
						}

						// remove permission if needed
						toRemove := itsyouonline.Permission{
							Read:   currentPermision.Read && !requestedPermission.Read,
							Write:  currentPermision.Write && !requestedPermission.Write,
							Delete: currentPermision.Delete && !requestedPermission.Delete,
							Admin:  currentPermision.Admin && !requestedPermission.Admin,
						}
						if err := nsMgr.RemovePermission(namespace, user, toRemove); err != nil {
							return cli.NewExitError(err, 1)
						}

						// Give requested permission
						if err := nsMgr.GivePermission(namespace, user, requestedPermission); err != nil {
							return cli.NewExitError(err, 1)
						}

						return nil
					},
				},
				{
					Name:  "get-acl",
					Usage: "Get the permission of a user",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "namespace",
							Usage: "Label of the namespace",
						},
						cli.StringFlag{
							Name:  "user",
							Usage: "ItsYouOnline user id",
						},
					},
					Action: func(c *cli.Context) error {
						namespace := c.String("namespace")
						user := c.String("user")
						perm, err := nsMgr.GetPermission(namespace, user)
						if err != nil {
							return cli.NewExitError(fmt.Errorf("fail to retrieve permission : %v", err), 1)
						}
						fmt.Printf("User %s:\n", user)
						fmt.Printf("Read: %v\n", perm.Read)
						fmt.Printf("Write: %v\n", perm.Write)
						fmt.Printf("Delete: %v\n", perm.Delete)
						fmt.Printf("Admin: %v\n", perm.Admin)

						return nil
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err.Error())
	}
}
func getClient(c *cli.Context) (*client.Client, error) {
	conf, err := readConfig()
	if err != nil {
		return nil, err
	}
	// create client
	cl, err := client.New(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	return cl, nil
}

func getNamespaceManager(c *cli.Context) (itsyouonline.NamespaceManager, error) {
	conf, err := readConfig()
	if err != nil {
		return nil, err
	}

	return itsyouonline.NewClient(conf.Organization, conf.IYOAppID, conf.IYOSecret), nil
}

func readConfig() (*config.Config, error) {
	// read config
	f, err := os.Open(confFile)
	if err != nil {
		return nil, err
	}

	return config.NewFromReader(f)
}
