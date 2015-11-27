package main

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/up-neoutil-go"
	"github.com/gorilla/mux"
	"github.com/jmcvetta/neoism"
	"log"
	"net/http"
	"os"
	"os/signal"
)

func main() {

	neoURL := os.Getenv("NEO_URL")
	if neoURL == "" {
		log.Println("no $NEO_URL set, defaulting to local")
		neoURL = "http://localhost:7474/db/data"
	}
	log.Printf("connecting to %s\n", neoURL)

	var err error
	db, err = neoism.Connect(neoURL)
	if err != nil {
		panic(err)
	}

	neoutil.EnsureIndexes(db, map[string]string{
		"Organisation": "uuid",
		"Concept":      "uuid",
		"Industry":     "uuid",
	})

	port := 8080

	m := mux.NewRouter()
	http.Handle("/", m)

	m.HandleFunc("/organisations/{uuid}", writeHandler).Methods("PUT")

	go func() {
		log.Printf("listening on %d", port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Printf("web stuff failed: %v\n", err)
		}
	}()

	bw = neoutil.NewBatchWriter(db, 1024)

	// wait for ctrl-c
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	close(bw.WriteQueue)
	<-bw.Closed

	log.Println("exiting")
}

var db *neoism.Database

var bw *neoutil.BatchWriter

func writeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	var o organisation
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&o)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if o.UUID != uuid {
		fmt.Printf("%v\n", o)
		http.Error(w, fmt.Sprintf("id does not match: %v %v", o.UUID, uuid), http.StatusBadRequest)
		return
	}

	bw.WriteQueue <- toQueries(o)

	w.WriteHeader(http.StatusAccepted)
}

func toQueries(o organisation) []*neoism.CypherQuery {
	p := map[string]interface{}{
		"uuid": o.UUID,
	}

	if o.Extinct == true {
		p["extinct"] = true
	}
	if o.FormerNames != nil && len(o.FormerNames) != 0 {
		p["formerNames"] = o.FormerNames
	}
	if o.HiddenLabel != "" {
		p["hiddenLabel"] = o.HiddenLabel
	}
	if o.LegalName != "" {
		p["legalName"] = o.LegalName
	}
	if o.LocalNames != nil && len(o.LocalNames) != 0 {
		p["localNames"] = o.LocalNames
	}
	if o.ProperName != "" {
		p["properName"] = o.ProperName
		p["prefLabel"] = o.ProperName
	}
	if o.ShortName != "" {
		p["shortName"] = o.ShortName
	}
	if o.TradeNames != nil && len(o.TradeNames) != 0 {
		p["tradeNames"] = o.TradeNames
	}
	for _, identifier := range o.Identifiers {
		if identifier.Authority == fsAuthority {
			p["factsetIdentifier"] = identifier.IdentifierValue
		}
		if identifier.Authority == leiAuthority {
			p["leiIdentifier"] = identifier.IdentifierValue
		}
	}
	p["uuid"] = o.UUID

	parms := map[string]interface{}{
		"uuid":     o.UUID,
		"allProps": neoism.Props(p),
		"puuid":    o.ParentOrganisation,
		"icuuid":   o.IndustryClassification,
	}

	statement := `
			MERGE (n:Concept {uuid: {uuid}})
			SET n = {allProps}
			SET n :Organisation
		`

	if o.Type != "Organisation" && o.Type != "" {
		statement += fmt.Sprintf("SET n :%s\n", o.Type)
	}

	if o.ParentOrganisation != "" {
		statement += `
			MERGE (p:Concept {uuid: {puuid}})
			MERGE (n)-[:SUB_ORG_OF]->(p)
			SET p :Organisation
		`
	}

	if o.IndustryClassification != "" {
		statement += `
			MERGE (ic:Concept {uuid: {icuuid}})
			MERGE (n)-[:IN_INDUSTRY]->(ic)
			SET ic :Industry
		`
	}

	return []*neoism.CypherQuery{
		&neoism.CypherQuery{Statement: statement, Parameters: parms},
	}
}

const (
	fsAuthority  = "http://api.ft.com/system/FACTSET-EDM"
	leiAuthority = "http://api.ft.com/system/LEI"
)
