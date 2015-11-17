package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/jmcvetta/neoism"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
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

	ensureIndexes(db)

	writeQueue = make(chan []*neoism.CypherQuery, 2048)

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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		orgWriteLoop()
		wg.Done()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// wait for ctrl-c
	<-c
	close(writeQueue)
	wg.Wait()
	println("exiting")

}

func ensureIndexes(db *neoism.Database) {
	ensureIndex(db, "Organisation", "uuid")
	ensureIndex(db, "Concept", "uuid")
}

func ensureIndex(db *neoism.Database, label string, prop string) {
	indexes, err := db.Indexes(label)
	if err != nil {
		panic(err)
	}
	for _, ind := range indexes {
		if len(ind.PropertyKeys) == 1 && ind.PropertyKeys[0] == prop {
			return
		}
	}
	if _, err := db.CreateIndex(label, prop); err != nil {
		panic(err)
	}
}

var db *neoism.Database

var writeQueue chan []*neoism.CypherQuery

func orgWriteLoop() {
	var qs []*neoism.CypherQuery

	timer := time.NewTimer(1 * time.Second)

	defer log.Println("write loop exited")
	for {
		select {
		case o, ok := <-writeQueue:
			if !ok {
				return
			}
			for _, q := range o {
				qs = append(qs, q)
			}
			if len(qs) < 1024 {
				timer.Reset(1 * time.Second)
				continue
			}
		case <-timer.C:
		}
		if len(qs) > 0 {
			fmt.Printf("writing batch of %d\n", len(qs))
			err := db.CypherBatch(qs)
			if err != nil {
				panic(err)
			}
			fmt.Printf("wrote batch of %d\n", len(qs))
			qs = qs[0:0]
			timer.Stop()
		}
	}
}

func allWriteHandler(w http.ResponseWriter, r *http.Request) {

	dec := json.NewDecoder(r.Body)

	for {
		var o organisation
		err := dec.Decode(&o)
		if err == io.ErrUnexpectedEOF {
			println("eof")
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		writeQueue <- toQueries(o)
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

	writeQueue <- toQueries(o)

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
			MERGE (p:Organisation {uuid: {puuid}})
			MERGE (n)-[r:SUB_ORG_OF]->(p)
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
			MERGE (ic:Industry {uuid: {icuuid}})
			MERGE (n)-[r:IN_INDUSTRY]->(ic)
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
