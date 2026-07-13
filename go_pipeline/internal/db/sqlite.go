package db

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/ericguerrize/pncp-go/internal/config"
	"github.com/ericguerrize/pncp-go/internal/models"
	_ "modernc.org/sqlite"
)

var dbPath = filepath.Join(config.BaseDir, "monitor_pncp.db")

func GetConnection() (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func InicializarBanco() error {
	db, err := GetConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	log.Printf("Inicializando banco de dados em: %s\n", dbPath)

	queries := []string{
		`CREATE TABLE IF NOT EXISTS pncp_data (
			id TEXT PRIMARY KEY,
			municipio TEXT,
			orgao TEXT,
			cnpj TEXT,
			modalidade TEXT,
			numero TEXT,
			ano TEXT,
			objeto TEXT,
			valor REAL,
			data_publicacao DATE,
			raw_json TEXT,
			criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS aplic_data (
			id TEXT PRIMARY KEY,
			municipio TEXT,
			orgao TEXT,
			cnpj TEXT,
			modalidade TEXT,
			modalidade_cod TEXT,
			numero TEXT,
			ano TEXT,
			objeto TEXT,
			valor REAL,
			data_abertura TEXT,
			ug_code TEXT,
			criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS crossmatch_results (
			id_pncp TEXT,
			id_aplic TEXT,
			municipio TEXT,
			status_cruzamento TEXT,
			score_composto REAL,
			estrategia_match TEXT,
			sincronizado_firebase INTEGER DEFAULT 0,
			data_cruzamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id_pncp, id_aplic)
		)`,
	}

	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	log.Println("Tabelas criadas/verificadas com sucesso.")
	return nil
}

func SalvarPNCP(data []models.ProcessedCompra) error {
	if len(data) == 0 {
		return nil
	}

	db, err := GetConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	// Load existing IDs
	rows, err := db.Query("SELECT id FROM pncp_data")
	if err != nil {
		return err
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			existing[id] = true
		}
	}

	// Prepare insertion
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO pncp_data 
		(id, municipio, orgao, cnpj, modalidade, numero, ano, objeto, valor, data_publicacao) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	insertedCount := 0
	for _, row := range data {
		// Clean ID
		id := strings.ReplaceAll(row.NumeroControlePNCP, "/", "_")
		if existing[id] {
			continue
		}

		// Clean CNPJ (numbers only)
		cnpj := strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' {
				return r
			}
			return -1
		}, row.OrgaoEntidadeCNPJ)

		// Valor
		valor := row.ValorTotalHomologado
		if valor == 0 {
			valor = row.ValorTotalEstimado
		}

		_, err = stmt.Exec(
			id,
			row.MunicipioNome,
			row.NomeUnidade,
			cnpj,
			row.ModalidadeNome,
			row.NumeroCompra,
			fmt.Sprintf("%d", row.AnoCompra),
			row.ObjetoCompra,
			valor,
			row.DataPublicacaoPncp.Format("2006-01-02"),
		)
		if err != nil {
			log.Printf("Erro ao inserir PNCP ID %s: %v\n", id, err)
			continue
		}
		existing[id] = true // Prevent duplicates in the same batch
		insertedCount++
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if insertedCount > 0 {
		log.Printf("%d novos registros PNCP salvos no banco.\n", insertedCount)
	} else {
		log.Println("Nenhum registro PNCP novo para salvar.")
	}

	return nil
}
