package models

import "time"

// PNCPResponse represents the paginated response from the PNCP API
type PNCPResponse struct {
	Data         []Compra `json:"data"`
	TotalPaginas int      `json:"totalPaginas"`
	TotalRegistros int    `json:"totalRegistros"`
}

// Compra represents a single procurement (licitação) in the PNCP system
type Compra struct {
	DataInclusao       string  `json:"dataInclusao"`
	DataPublicacaoPncp string  `json:"dataPublicacaoPncp"`
	DataAtualizacao    string  `json:"dataAtualizacao"`
	UsuarioNome        string  `json:"usuarioNome"`
	NumeroCompra       string  `json:"numeroCompra"`
	AnoCompra          int     `json:"anoCompra"`
	Processo           string  `json:"processo"`
	ModalidadeId       int     `json:"modalidadeId"`
	ModalidadeNome     string  `json:"modalidadeNome"`
	ModoDisputaNome    string  `json:"modoDisputaNome"`
	SituacaoCompraId   int     `json:"situacaoCompraId"`
	SituacaoCompraNome string  `json:"situacaoCompraNome"`
	ObjetoCompra       string  `json:"objetoCompra"`
	ValorTotalEstimado float64 `json:"valorTotalEstimado"`
	ValorTotalHomologado float64 `json:"valorTotalHomologado"`
	NumeroControlePNCP string  `json:"numeroControlePNCP"`

	OrgaoEntidade struct {
		CNPJ        string `json:"cnpj"`
		RazaoSocial string `json:"razaoSocial"`
		EsferaId    string `json:"esferaId"`
	} `json:"orgaoEntidade"`

	UnidadeOrgao struct {
		MunicipioNome string `json:"municipioNome"`
		NomeUnidade   string `json:"nomeUnidade"`
		CodigoUnidade string `json:"codigoUnidade"`
		CodigoIbge    string `json:"codigoIbge"`
	} `json:"unidadeOrgao"`
}

// ProcessedCompra is the flattened structure used for Excel/Database/Crossmatch
type ProcessedCompra struct {
	DataInclusao       time.Time
	DataPublicacaoPncp time.Time
	DataAtualizacao    time.Time
	OrgaoEntidadeCNPJ  string
	OrgaoEntidadeRazao string
	MunicipioNome      string
	NomeUnidade        string
	CodigoUnidade      string
	CodigoIbge         string
	UsuarioNome        string
	NumeroCompra       string
	AnoCompra          int
	Processo           string
	ModalidadeId       int
	ModalidadeNome     string
	ModoDisputaNome    string
	SituacaoCompraId   int
	SituacaoCompraNome string
	ObjetoCompra       string
	ValorTotalEstimado float64
	ValorTotalHomologado float64
	NumeroControlePNCP string
}
