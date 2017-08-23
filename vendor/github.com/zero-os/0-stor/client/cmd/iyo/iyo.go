package main

import (
	"log"
	"os"

	"github.com/codegangsta/cli"
)

var (
	jwtCmd = &jwtCommand{}
)

func main() {
	app := cli.NewApp()
	app.Name = "iyo client tester"
	app.Commands = []cli.Command{
		{
			Name:  "jwt",
			Usage: "test jwt",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "client_id",
					Usage:       "client ID",
					Destination: &jwtCmd.clientID,
				},
				cli.StringFlag{
					Name:        "secret",
					Usage:       "client secret",
					Destination: &jwtCmd.secret,
				},

				cli.StringFlag{
					Name:        "org",
					Usage:       "organization",
					Destination: &jwtCmd.org,
				},
				cli.StringFlag{
					Name:        "namespace",
					Usage:       "0-stor namespace",
					Destination: &jwtCmd.namespace,
				},
				cli.BoolFlag{
					Name:        "read",
					Usage:       "read permission",
					Destination: &jwtCmd.read,
				},
				cli.BoolFlag{
					Name:        "write",
					Usage:       "write permission",
					Destination: &jwtCmd.write,
				},
				cli.BoolFlag{
					Name:        "delete",
					Usage:       "delete permission",
					Destination: &jwtCmd.delete,
				},
			},
			Action: func(c *cli.Context) {
				if err := jwtCmd.Execute(); err != nil {
					log.Fatal(err)
				}
			},
		},
	}
	app.Action = func(c *cli.Context) {
		cli.ShowAppHelp(c)
	}

	app.Run(os.Args)

}
