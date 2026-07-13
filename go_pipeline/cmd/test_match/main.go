package main

import (
	"fmt"
	"github.com/ericguerrize/pncp-go/internal/crossmatch"
	"path/filepath"
	"github.com/ericguerrize/pncp-go/internal/config"
)

func main() {
	csvPath := filepath.Join(config.BaseDir, "..", "python_backup", "pncp_pipeline", "input", "licitacao_lucas_do_rio_verde_2026.csv")
	aplicData, err := crossmatch.CarregarAplicCSV(csvPath)
	if err != nil {
		fmt.Println("Erro:", err)
		return
	}
	for i := 0; i < 3; i++ {
		fmt.Printf("APLIC[%d] -> Municipio: '%s', Ano: %d, Numero: '%s', Objeto: '%s'\n", i, aplicData[i].Municipio, aplicData[i].Ano, aplicData[i].Numero, aplicData[i].Objetivo)
	}
}
