package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/urfave/cli"
	"github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/itsyouonline"
)

var (
	confFile string

	// CommitHash represents the Git commit hash at built time
	CommitHash string
	// BuildDate represents the date when this tool suite was built
	BuildDate string
)

func outputVersion() string {
	// Tool Version
	version := "Version: " + "1.1.0-alpha-8"

	// Build (Git) Commit Hash
	if CommitHash != "" {
		version += "\r\nBuild: " + CommitHash
		if BuildDate != "" {
			version += " " + BuildDate
		}
	}

	// Output version and runtime information
	return fmt.Sprintf("%s\r\nRuntime: %s %s\r\n",
		version,
		runtime.Version(), // Go Version
		runtime.GOOS,      // OS Name
	)
}

func main() {
	var cl *client.Client
	var iyoCl itsyouonline.IYOClient
	var key string

	app := cli.NewApp()
	app.Version = outputVersion()
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
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:        "key, k",
							Usage:       "key to use to store the file, if empty use the name of the file as key",
							Destination: &key,
						},
					},
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 1 {
							return cli.NewExitError("need to give the path to the file to upload", 1)
						}

						fileName := c.Args().First()

						f, err := os.Open(fileName)
						if err != nil {
							return cli.NewExitError(fmt.Errorf("can't read the file: %v", err), 1)
						}
						defer f.Close()

						if key == "" {
							key = filepath.Base(fileName)
						}

						_, err = cl.WriteF([]byte(key), f)
						if err != nil {
							return cli.NewExitError(fmt.Errorf("upload failed : %v", err), 1)
						}
						fmt.Printf("file uploaded, key = %v\n", key)
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

						if err := cl.ReadF([]byte(key), fOutput); err != nil {
							return cli.NewExitError(fmt.Errorf("download file failed: %v", err), 1)
						}

						fmt.Printf("file downloaded to %s\n", output)

						return nil
					},
				},
				{
					Name:  "metadata",
					Usage: "print the metadata of a key",
					Action: func(c *cli.Context) error {
						if len(c.Args()) < 1 {
							return cli.NewExitError(fmt.Errorf("need to give the key of the object to inspect"), 1)
						}

						key := c.Args().Get(0)
						if key == "" {
							return cli.NewExitError("key cannot be empty", 1)
						}

						meta, err := cl.GetMeta([]byte(key))
						if err != nil {
							return cli.NewExitError(fmt.Sprintf("fail to get metadata: %v", err), 1)
						}

						b, err := json.Marshal(meta)
						if err != nil {
							return cli.NewExitError("error encoding metadata into json", 1)
						}
						fmt.Print(string(b))

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
				iyoCl, err = getNamespaceManager(c)
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
						if err := iyoCl.CreateNamespace(namespace); err != nil {
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
						if err := iyoCl.DeleteNamespace(namespace); err != nil {
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
						currentPermision, err := iyoCl.GetPermission(namespace, user)
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
						if err := iyoCl.RemovePermission(namespace, user, toRemove); err != nil {
							return cli.NewExitError(err, 1)
						}

						// Give requested permission
						if err := iyoCl.GivePermission(namespace, user, requestedPermission); err != nil {
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
						perm, err := iyoCl.GetPermission(namespace, user)
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
	policy, err := readPolicy()
	if err != nil {
		return nil, err
	}
	// create client
	cl, err := client.New(policy)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	return cl, nil
}

func getNamespaceManager(c *cli.Context) (itsyouonline.IYOClient, error) {
	policy, err := readPolicy()
	if err != nil {
		return nil, err
	}

	return itsyouonline.NewClient(policy.Organization, policy.IYOAppID, policy.IYOSecret), nil
}

func readPolicy() (client.Policy, error) {
	// read config
	f, err := os.Open(confFile)
	if err != nil {
		return client.Policy{}, err
	}

	return client.NewPolicyFromReader(f)
}
