package orgs

type Organisation struct {
	Extinct                bool         `json:"extinct,omitempty"`
	FormerNames            []string     `json:"formerNames,omitempty"`
	HiddenLabel            string       `json:"hiddenLabel,omitempty"`
	Identifiers            []Identifier `json:"identifiers,omitempty"`
	IndustryClassification string       `json:"industryClassification,omitempty"`
	LegalName              string       `json:"legalName,omitempty"`
	LocalNames             []string     `json:"localNames,omitempty"`
	ParentOrganisation     string       `json:"parentOrganisation,omitempty"`
	ProperName             string       `json:"properName,omitempty"`
	ShortName              string       `json:"shortName,omitempty"`
	TradeNames             []string     `json:"tradeNames,omitempty"`
	Type                   string       `json:"type,omitempty"`
	UUID                   string       `json:"uuid"`
}

type Identifier struct {
	Authority       string `json:"authority"`
	IdentifierValue string `json:"identifierValue"`
}
