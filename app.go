package main

import (
	"github.com/Financial-Times/org-writer-neo/orgs"
	"github.com/Financial-Times/up-neoutil-go"
	"github.com/jawher/mow.cli"
	"os"
)

func main() {

	engs := map[string]neoutil.NeoEngine{
		"organisations": orgs.RolesNeoEngine{},
	}

	app := cli.App("organisations-api-neo", "A RESTful API for managing Organisations in neo4j")
	neoURL := app.StringOpt("neo-url", "http://localhost:7474/db/data", "neo4j endpoint URL")
	port := app.IntOpt("port", 8080, "Port to listen on")

	app.Action = func() {
		neoutil.RunServer(engs, *neoURL, *port)
	}

	app.Run(os.Args)
}
