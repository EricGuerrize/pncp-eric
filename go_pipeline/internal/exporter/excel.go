package exporter

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/ericguerrize/pncp-go/internal/config"
	"github.com/ericguerrize/pncp-go/internal/models"
	"github.com/xuri/excelize/v2"
)

// ExportToExcel takes normalized data and generates an XLSX file.
func ExportToExcel(data []models.ProcessedCompra, fileDate string) error {
	log.Printf("Iniciando exportação para Excel: %d registros\n", len(data))

	f := excelize.NewFile()
	sheet := "Resultados"
	
	// Create a new sheet
	index, err := f.NewSheet(sheet)
	if err != nil {
		return err
	}
	f.SetActiveSheet(index)
	f.DeleteSheet("Sheet1") // Remove default sheet

	// Write Headers
	headers := []interface{}{
		"DataInclusao", "DataPublicacaoPncp", "OrgaoEntidade_cnpj", "OrgaoEntidade_razaoSocial",
		"UnidadeOrgao_municipioNome", "UnidadeOrgao_nomeUnidade", "UnidadeOrgao_codigoUnidade",
		"UnidadeOrgao_codigoIbge", "UsuarioNome", "NumeroCompra", "AnoCompra", "Processo",
		"ModalidadeId", "ModalidadeNome", "ModoDisputaNome", "SituacaoCompraId",
		"SituacaoCompraNome", "ObjetoCompra", "ValorTotalEstimado", "ValorTotalHomologado",
		"NumeroControlePNCP", "DataAtualizacao",
	}

	if err := f.SetSheetRow(sheet, "A1", &headers); err != nil {
		return fmt.Errorf("failed to write headers: %w", err)
	}

	// Write Data
	for i, row := range data {
		cell, _ := excelize.CoordinatesToCellName(1, i+2)
		rowData := []interface{}{
			row.DataInclusao.Format("2006-01-02 15:04:05"),
			row.DataPublicacaoPncp.Format("2006-01-02 15:04:05"),
			row.OrgaoEntidadeCNPJ,
			row.OrgaoEntidadeRazao,
			row.MunicipioNome,
			row.NomeUnidade,
			row.CodigoUnidade,
			row.CodigoIbge,
			row.UsuarioNome,
			row.NumeroCompra,
			row.AnoCompra,
			row.Processo,
			row.ModalidadeId,
			row.ModalidadeNome,
			row.ModoDisputaNome,
			row.SituacaoCompraId,
			row.SituacaoCompraNome,
			row.ObjetoCompra,
			row.ValorTotalEstimado,
			row.ValorTotalHomologado,
			row.NumeroControlePNCP,
			row.DataAtualizacao.Format("2006-01-02 15:04:05"),
		}
		if err := f.SetSheetRow(sheet, cell, &rowData); err != nil {
			log.Printf("Warning: failed to write row %d: %v\n", i+2, err)
		}
	}

	// Format filename
	filename := fmt.Sprintf("pncp_contratacoes_MT_%s.xlsx", fileDate)
	filePath := filepath.Join(config.OutputDir, filename)

	if err := f.SaveAs(filePath); err != nil {
		return fmt.Errorf("failed to save excel file: %w", err)
	}

	log.Printf("Arquivo Excel gerado com sucesso: %s\n", filePath)
	return nil
}
