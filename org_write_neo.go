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

	var err error
	db, err = neoism.Connect("http://localhost:7474/db/data")
	if err != nil {
		panic(err)
	}

	ensureIndexes(db)

	writeQueue = make(chan organisation, 2048)

	port := 8082

	m := mux.NewRouter()
	http.Handle("/", m)

	m.HandleFunc("/organisations/{uuid}", idWriteHandler).Methods("PUT")
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

	indexes, err := db.Indexes("Organisation")
	if err != nil {
		panic(err)
	}
	if len(indexes) == 0 {
		if _, err := db.CreateIndex("Organisation", "uuid"); err != nil {
			panic(err)
		}
		if _, err := db.CreateIndex("Industry", "uuid"); err != nil {
			panic(err)
		}
	}
}

var db *neoism.Database

var writeQueue chan organisation

func orgWriteLoop() {
	var qs []*neoism.CypherQuery

	timer := time.NewTimer(1 * time.Second)

	defer println("asdasd")
	for {
		select {
		case o, ok := <-writeQueue:
			if !ok {
				return
			}
			for _, q := range toQueries(o) {
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

		writeQueue <- o
	}

	w.WriteHeader(http.StatusAccepted)
}

func idWriteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	UUID := vars["uuid"]

	var o organisation
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&o)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if o.UUID != UUID {
		fmt.Printf("%v\n", o)
		http.Error(w, fmt.Sprintf("id does not match: %v %v", o.UUID, UUID), http.StatusBadRequest)
		return
	}

	writeQueue <- o

	w.WriteHeader(http.StatusAccepted)
}

func toQueries(o organisation) []*neoism.CypherQuery {
	props := toProps(o)

	var queries []*neoism.CypherQuery

	queries = append(queries, &neoism.CypherQuery{
		Statement: `
			MERGE (n:Organisation {uuid: {uuid}})
			SET n = {allProps}
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

	if o.LegalName != "" {
		p["legalName"] = o.LegalName
	}
	if o.ProperName != "" {
		p["properName"] = o.ProperName
	}
	if o.FormerNames != nil {
		p["formerNames"] = o.FormerNames
	}
	if o.TradeNames != nil {
		p["tradeNames"] = o.TradeNames
	}
	if o.Extinct == true {
		p["extinct"] = true
	}
	// TODO: finish this.

	return neoism.Props(p)
}
