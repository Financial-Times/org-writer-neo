package main

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/up-neoutil-go"
	"github.com/gorilla/mux"
	"github.com/jmcvetta/neoism"
	"io"
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
	m.HandleFunc("/organisations/", allWriteHandler).Methods("PUT")

	go func() {
		log.Printf("listening on %d", port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Printf("web stuff failed: %v\n", err)
		}
	}()

	bw = neoutil.NewBatchWriter(db)

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

func allWriteHandler(w http.ResponseWriter, r *http.Request) {

	dec := json.NewDecoder(r.Body)

	for {
		var o organisation
		err := dec.Decode(&o)
		if err == io.ErrUnexpectedEOF {
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		bw.WriteQueue <- toQueries(o)
	}

	w.WriteHeader(http.StatusAccepted)
}

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
	props := toProps(o)

	var queries []*neoism.CypherQuery

	queries = append(queries, &neoism.CypherQuery{
		Statement: `
			MERGE (n:Concept {uuid: {uuid}})
			SET n = {allProps}
			SET n :Organisation
			RETURN n
		`,
		Parameters: map[string]interface{}{
			"uuid":     o.UUID,
			"allProps": props,
		},
	})

	t := string(o.Type)
	if t != "Organisation" && t != "" {
		queries = append(queries, &neoism.CypherQuery{
			Statement: fmt.Sprintf("MERGE (n:Organisation {uuid: {uuid}}) SET n :%s", t),
			Parameters: map[string]interface{}{
				"uuid": o.UUID,
			},
		})
	}

	if o.ParentOrganisation != "" {
		queries = append(queries, &neoism.CypherQuery{
			Statement: `
			MERGE (n:Organisation {uuid: {uuid}})
			MERGE (p:Concept {uuid: {puuid}})
			MERGE (n)-[r:SUB_ORG_OF]->(p)
			SET p :Organisation
		`,
			Parameters: map[string]interface{}{
				"uuid":  o.UUID,
				"puuid": o.ParentOrganisation,
			},
		})
	}

	if o.IndustryClassification != "" {
		queries = append(queries, &neoism.CypherQuery{
			Statement: `
			MERGE (n:Organisation {uuid: {uuid}})
			MERGE (ic:Concept {uuid: {icuuid}})
			MERGE (n)-[r:IN_INDUSTRY]->(ic)
			SET ic :Industry
		`,
			Parameters: map[string]interface{}{
				"uuid":   o.UUID,
				"icuuid": o.IndustryClassification,
			},
		})

	}

	return queries
}

func toProps(o organisation) neoism.Props {
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
	}
	p["uuid"] = o.UUID

	return neoism.Props(p)
}

const (
	fsAuthority = "http://api.ft.com/system/FACTSET-EDM"
)
