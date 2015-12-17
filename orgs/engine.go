package orgs

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/up-neoutil-go"
	"github.com/jmcvetta/neoism"
	"strings"
)

type RolesNeoEngine struct{}

func (bnc RolesNeoEngine) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	b := Organisation{}
	err := dec.Decode(&b)
	return b, b.UUID, err
}

func (bnc RolesNeoEngine) SuggestedIndexes() map[string]string {
	return map[string]string{
		"Organisation": "uuid",
		"Concept":      "uuid",
		"Industry":     "uuid",
	}
}

func (bnc RolesNeoEngine) Read(cr neoutil.CypherRunner, identity string) (interface{}, bool, error) {
	panic("not implemented")
}

func (bnc RolesNeoEngine) CreateOrUpdate(cr neoutil.CypherRunner, obj interface{}) error {
	o := obj.(Organisation)

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

	queries := []*neoism.CypherQuery{
		&neoism.CypherQuery{Statement: statement, Parameters: parms},
	}

	return cr.CypherBatch(queries)
}

func (bnc RolesNeoEngine) Delete(cr neoutil.CypherRunner, identity string) (bool, error) {
	panic("not implemented")
}

func uriToUUID(uri string) string {
	// TODO: make this more robust
	return strings.Replace(uri, "http://api.ft.com/things/", "", 1)
}

const (
	fsAuthority  = "http://api.ft.com/system/FACTSET-EDM"
	leiAuthority = "http://api.ft.com/system/LEI"
)
