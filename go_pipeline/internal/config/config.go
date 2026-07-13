package config

import (
	"os"
	"path/filepath"
)

// Base Directories
var (
	BaseDir   string
	LogsDir   string
	OutputDir string
	InputDir  string
)

func init() {
	// Initialize paths relative to the current working directory or executable
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}
	BaseDir = cwd
	LogsDir = filepath.Join(BaseDir, "logs")
	OutputDir = filepath.Join(BaseDir, "output")
	InputDir = filepath.Join(BaseDir, "input")

	// Create directories if they don't exist
	os.MkdirAll(LogsDir, os.ModePerm)
	os.MkdirAll(OutputDir, os.ModePerm)
	os.MkdirAll(InputDir, os.ModePerm)
}

// API Configuration
const (
	PNCPBaseURL          = "https://pncp.gov.br/api/consulta/v1"
	ContratacoesEndpoint = "/contratacoes/publicacao"
)

// Query Parameters
const (
	UF             = "MT"
	TamanhoPagina  = 50
)

// Concurrency & Retry Settings
const (
	MaxConcurrentRequests = 10
	MaxRetries            = 5
	RequestTimeoutSeconds = 60
)

// Mapping of Modalidades
var Modalidades = map[int]string{
	1:  "Leilão - Eletrônico",
	2:  "Diálogo Competitivo",
	3:  "Concurso",
	4:  "Concorrência - Eletrônica",
	5:  "Concorrência - Presencial",
	6:  "Pregão - Eletrônico",
	7:  "Pregão - Presencial",
	8:  "Dispensa de Licitação",
	9:  "Inexigibilidade",
	10: "Manifestação de Interesse",
	11: "Pré-qualificação",
	12: "Credenciamento",
	13: "Leilão - Presencial",
}
