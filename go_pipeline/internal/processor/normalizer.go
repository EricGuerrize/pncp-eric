package processor

import (
	"log"
	"time"

	"github.com/ericguerrize/pncp-go/internal/collector"
	"github.com/ericguerrize/pncp-go/internal/models"
)

// parsePNCPDate parses PNCP API timestamps, which are usually returned without a
// timezone offset (e.g. "2025-03-19T12:05:18") but occasionally include one.
func parsePNCPDate(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// NormalizeResults takes the raw concurrent responses and flattens them into ProcessedCompra
func NormalizeResults(rawResults []collector.CollectResult, execTime time.Time) []models.ProcessedCompra {
	var normalized []models.ProcessedCompra
	log.Println("Normalizando resultados...")

	for _, result := range rawResults {
		if result.Error != nil || result.Response == nil {
			continue
		}

		for _, item := range result.Response.Data {
			dataInclusao := parsePNCPDate(item.DataInclusao)
			dataPub := parsePNCPDate(item.DataPublicacaoPncp)
			dataAtual := parsePNCPDate(item.DataAtualizacao)

			flat := models.ProcessedCompra{
				DataInclusao:         dataInclusao,
				DataPublicacaoPncp:   dataPub,
				DataAtualizacao:      dataAtual,
				OrgaoEntidadeCNPJ:    item.OrgaoEntidade.CNPJ,
				OrgaoEntidadeRazao:   item.OrgaoEntidade.RazaoSocial,
				MunicipioNome:        item.UnidadeOrgao.MunicipioNome,
				NomeUnidade:          item.UnidadeOrgao.NomeUnidade,
				CodigoUnidade:        item.UnidadeOrgao.CodigoUnidade,
				CodigoIbge:           item.UnidadeOrgao.CodigoIbge,
				UsuarioNome:          item.UsuarioNome,
				NumeroCompra:         item.NumeroCompra,
				AnoCompra:            item.AnoCompra,
				Processo:             item.Processo,
				ModalidadeId:         item.ModalidadeId,
				ModalidadeNome:       item.ModalidadeNome,
				ModoDisputaNome:      item.ModoDisputaNome,
				SituacaoCompraId:     item.SituacaoCompraId,
				SituacaoCompraNome:   item.SituacaoCompraNome,
				ObjetoCompra:         item.ObjetoCompra,
				ValorTotalEstimado:   item.ValorTotalEstimado,
				ValorTotalHomologado: item.ValorTotalHomologado,
				NumeroControlePNCP:   item.NumeroControlePNCP,
			}

			// Apply the cleaning rules directly here (from dataset_builder.py)
			// e.g. df = df[df['orgaoEntidade_esferaId'].isin(['M', 'E'])]
			esfera := item.OrgaoEntidade.EsferaId
			if esfera == "M" || esfera == "E" || esfera == "" { // sometimes empty depending on data
				normalized = append(normalized, flat)
			}
		}
	}

	log.Printf("Total de registros normalizados: %d\n", len(normalized))
	return normalized
}
