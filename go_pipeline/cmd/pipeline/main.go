package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ericguerrize/pncp-go/internal/api"
	"github.com/ericguerrize/pncp-go/internal/collector"
	"github.com/ericguerrize/pncp-go/internal/crossmatch"
	"github.com/ericguerrize/pncp-go/internal/db"
	"github.com/ericguerrize/pncp-go/internal/exporter"
	"github.com/ericguerrize/pncp-go/internal/processor"
)

func main() {
	// Setup flags
	fromPtr := flag.String("from", "", "Data inicial (YYYYMMDD)")
	toPtr := flag.String("to", "", "Data final (YYYYMMDD)")
	flag.Parse()

	log.Println("==================================================")
	log.Println("INICIANDO PIPELINE DE COLETA PNCP (GO)")

	// Determine dates
	now := time.Now()
	dataInicial := *fromPtr
	if dataInicial == "" {
		dataInicial = now.AddDate(0, 0, -1).Format("20060102")
	}

	dataFinal := *toPtr
	if dataFinal == "" {
		dataFinal = dataInicial
	}

	fileDate := dataInicial
	if dataInicial != dataFinal {
		fileDate = fmt.Sprintf("%s_%s", dataInicial, dataFinal)
	}

	log.Printf("Data consultada: %s a %s\n", dataInicial, dataFinal)

	// Step 0: Initialize DB
	if err := db.InicializarBanco(); err != nil {
		log.Fatalf("Falha ao inicializar o banco de dados: %v", err)
	}

	// Step 1: Collect
	apiClient := api.NewClient()
	col := collector.NewCollector(apiClient)
	
	startTime := time.Now()
	rawResults := col.CollectAllData(dataInicial, dataFinal, nil)

	// Step 2 & 3: Normalize & Clean
	normalized := processor.NormalizeResults(rawResults, now)

	if len(normalized) > 0 {
		// Step 4: Export to Excel
		if err := exporter.ExportToExcel(normalized, fileDate); err != nil {
			log.Printf("Erro ao exportar Excel: %v\n", err)
		}

		// Step 5: Save to SQLite
		if err := db.SalvarPNCP(normalized); err != nil {
			log.Printf("Erro ao salvar no SQLite: %v\n", err)
		}

		log.Printf("Pipeline (Base) concluído com sucesso. %d registros processados.\n", len(normalized))
		// Step 6: Crossmatch
		// Ao Vivo do Oracle TCE-MT para Lucas do Rio Verde 2026!
		aplicData, err := crossmatch.CarregarAplicOracleAoVivo("Lucas do Rio Verde", "2026")
		if err != nil {
			log.Printf("Erro ao carregar APLIC CSV: %v\n", err)
		} else {
			_, err = crossmatch.ExecutarCruzamento(normalized, aplicData)
			if err != nil {
				log.Printf("Erro ao executar Crossmatch: %v\n", err)
			} else {
				log.Println("Crossmatch concluído com sucesso e salvo no SQLite.")
			}
		}

		// TODO: Firebase sync
		log.Println("Nota: A sincronização com Firebase ainda está sendo desenvolvida.")
	} else {
		log.Println("Pipeline concluído. Nenhum registro encontrado para este período.")
	}

	elapsed := time.Since(startTime)
	log.Printf("Tempo total de execução: %.2f segundos\n", elapsed.Seconds())
	log.Println("==================================================")
}
