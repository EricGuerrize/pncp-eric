package crossmatch

import (
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/ericguerrize/pncp-go/internal/db"
	"github.com/ericguerrize/pncp-go/internal/models"
)

type MatchResult struct {
	IdPNCP            string
	IdAPLIC           string
	Municipio         string
	StatusCruzamento  string
	ScoreComposto     float64
	ScoreTexto        float64
	ScoreValor        float64
	ScoreData         float64
	DiferencaValor    float64
	DiferencaDataDias float64
	EstrategiaMatch   string
}

var objetoStopwords = map[string]bool{}

func init() {
	for _, w := range strings.Fields(
		"de da do das dos em no na nos nas para por com sem sob sobre entre ate apos ante e ou que se a o as os um uma uns umas ao aos " +
			"contratacao empresa especializada especializado municipio " +
			"conforme acordo termos especificacoes edital seus suas anexos " +
			"prestacao servicos execucao fim atender atendendo necessidades " +
			"secretaria municipal publica publico referente referentes objeto " +
			"presente processo sistema origem mt regime diversos diversas locais") {
		objetoStopwords[w] = true
	}
}

func SimilaridadeObjeto(a, b string) float64 {
	setA := distinctiveWords(a)
	setB := distinctiveWords(b)
	if len(setA) == 0 || len(setB) == 0 {
		return 0
	}

	intersection := 0
	union := make(map[string]bool, len(setA)+len(setB))
	for w := range setA {
		union[w] = true
		if setB[w] {
			intersection++
		}
	}
	for w := range setB {
		union[w] = true
	}

	return float64(intersection) / float64(len(union)) * 100
}

func distinctiveWords(s string) map[string]bool {
	words := make(map[string]bool)
	for _, w := range strings.Fields(normalizarTexto(s)) {
		if len(w) < 3 || objetoStopwords[w] {
			continue
		}
		words[w] = true
	}
	return words
}

func normalizarTexto(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = accentReplacer.Replace(s)
	s = punctuationReplacer.Replace(s)
	return strings.Join(strings.Fields(s), " ")
}

var accentReplacer = strings.NewReplacer(
	"á", "a", "à", "a", "ã", "a", "â", "a", "ä", "a",
	"é", "e", "è", "e", "ê", "e", "ë", "e",
	"í", "i", "ì", "i", "î", "i", "ï", "i",
	"ó", "o", "ò", "o", "õ", "o", "ô", "o", "ö", "o",
	"ú", "u", "ù", "u", "û", "u", "ü", "u",
	"ç", "c",
	"Ã¡", "a", "Ã ", "a", "Ã£", "a", "Ã¢", "a", "Ã¤", "a",
	"Ã©", "e", "Ã¨", "e", "Ãª", "e", "Ã«", "e",
	"Ã­", "i", "Ã¬", "i", "Ã®", "i", "Ã¯", "i",
	"Ã³", "o", "Ã²", "o", "Ãµ", "o", "Ã´", "o", "Ã¶", "o",
	"Ãº", "u", "Ã¹", "u", "Ã»", "u", "Ã¼", "u",
	"Ã§", "c",
)

var punctuationReplacer = strings.NewReplacer(
	".", " ",
	",", " ",
	";", " ",
	":", " ",
	"/", " ",
	"\\", " ",
	"-", " ",
	"_", " ",
	"(", " ",
	")", " ",
	"[", " ",
	"]", " ",
	"{", " ",
	"}", " ",
	"\"", " ",
	"'", " ",
	"\n", " ",
	"\r", " ",
	"\t", " ",
)

func NormalizeMunicipio(s string) string {
	return normalizarTexto(s)
}

func scoreValor(pncpValor, aplicValor float64) (float64, float64) {
	if pncpValor <= 0 || aplicValor <= 0 {
		return 50, 0
	}

	delta := math.Abs(pncpValor-aplicValor) / math.Max(pncpValor, aplicValor) * 100
	score := 100 - delta*4
	if score < 0 {
		score = 0
	}
	return score, delta
}

func scoreData(pncpData, aplicData time.Time) (float64, float64) {
	if pncpData.IsZero() || aplicData.IsZero() {
		return 50, 0
	}

	deltaDias := math.Abs(pncpData.Sub(aplicData).Hours() / 24)
	score := 100 - deltaDias*8
	if score < 0 {
		score = 0
	}
	return score, deltaDias
}

func scoreComposto(texto, valor, data float64) float64 {
	return texto*0.5 + valor*0.3 + data*0.2
}

func aplicarMatch(resultados *[]MatchResult, matchedPncp, matchedAplic map[string]bool, pncp models.ProcessedCompra, aplicIdx int, status string, texto float64, valor float64, data float64, difValor float64, difData float64, estrategia string) {
	matchedPncp[pncp.NumeroControlePNCP] = true
	matchedAplic[fmt.Sprintf("%d", aplicIdx)] = true

	*resultados = append(*resultados, MatchResult{
		IdPNCP:            pncp.NumeroControlePNCP,
		IdAPLIC:           fmt.Sprintf("APLIC-%d", aplicIdx),
		Municipio:         pncp.MunicipioNome,
		StatusCruzamento:  status,
		ScoreComposto:     scoreComposto(texto, valor, data),
		ScoreTexto:        texto,
		ScoreValor:        valor,
		ScoreData:         data,
		DiferencaValor:    difValor,
		DiferencaDataDias: difData,
		EstrategiaMatch:   estrategia,
	})
}

func valorPncpBase(pncp models.ProcessedCompra) float64 {
	if pncp.ValorTotalHomologado != 0 {
		return pncp.ValorTotalHomologado
	}
	return pncp.ValorTotalEstimado
}

func ExecutarCruzamento(pncpRecords []models.ProcessedCompra, aplicRecords []AplicData) ([]MatchResult, error) {
	log.Println("Iniciando Crossmatch (Go)...")
	var resultados []MatchResult

	matchedPncp := make(map[string]bool)
	matchedAplic := make(map[string]bool)

	for _, pncp := range pncpRecords {
		if matchedPncp[pncp.NumeroControlePNCP] {
			continue
		}

		valPncp := valorPncpBase(pncp)
		objPncpNorm := normalizarTexto(pncp.ObjetoCompra)
		munPncp := normalizarTexto(pncp.MunicipioNome)

		bestScore := 0.0
		bestTexto := 0.0
		bestValor := 0.0
		bestData := 0.0
		bestDiffValor := 0.0
		bestDiffData := 0.0
		bestAplicIdx := -1

		for j, aplic := range aplicRecords {
			if matchedAplic[fmt.Sprintf("%d", j)] {
				continue
			}
			if normalizarTexto(aplic.Municipio) != munPncp || aplic.Ano != pncp.AnoCompra {
				continue
			}

			texto := SimilaridadeObjeto(objPncpNorm, normalizarTexto(aplic.Objetivo))
			if texto < 40 {
				continue
			}

			valor, diffValor := scoreValor(valPncp, aplic.ValorEstimado)
			if diffValor > 10 && valPncp > 0 && aplic.ValorEstimado > 0 {
				continue
			}

			data, diffData := scoreData(pncp.DataPublicacaoPncp, aplic.DataAbertura)
			score := scoreComposto(texto, valor, data)
			if score > bestScore {
				bestScore = score
				bestTexto = texto
				bestValor = valor
				bestData = data
				bestDiffValor = diffValor
				bestDiffData = diffData
				bestAplicIdx = j
			}
		}

		if bestAplicIdx != -1 {
			aplicarMatch(&resultados, matchedPncp, matchedAplic, pncp, bestAplicIdx, "MATCH_CONFIRMADO", bestTexto, bestValor, bestData, bestDiffValor, bestDiffData, "primario_semantico")
		}
	}

	log.Printf("[Tier 1] %d matches semanticos\n", len(resultados))

	matchesTier2 := 0
	for _, pncp := range pncpRecords {
		if matchedPncp[pncp.NumeroControlePNCP] {
			continue
		}

		cnpjPncp := strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' {
				return r
			}
			return -1
		}, pncp.OrgaoEntidadeCNPJ)
		if cnpjPncp == "" {
			continue
		}

		valPncp := valorPncpBase(pncp)
		objPncpNorm := normalizarTexto(pncp.ObjetoCompra)

		bestScore := 0.0
		bestTexto := 0.0
		bestValor := 0.0
		bestData := 0.0
		bestDiffValor := 0.0
		bestDiffData := 9999.0
		bestAplicIdx := -1

		for j, aplic := range aplicRecords {
			if matchedAplic[fmt.Sprintf("%d", j)] {
				continue
			}
			if aplic.CNPJ != cnpjPncp {
				continue
			}

			texto := SimilaridadeObjeto(objPncpNorm, normalizarTexto(aplic.Objetivo))
			if texto < 30 {
				continue
			}

			valor, diffValor := scoreValor(valPncp, aplic.ValorEstimado)
			data, diffData := scoreData(pncp.DataPublicacaoPncp, aplic.DataAbertura)
			if !pncp.DataPublicacaoPncp.IsZero() && !aplic.DataAbertura.IsZero() && diffData > 10 {
				continue
			}

			score := scoreComposto(texto, valor, data)
			if score > bestScore {
				bestScore = score
				bestTexto = texto
				bestValor = valor
				bestData = data
				bestDiffValor = diffValor
				bestDiffData = diffData
				bestAplicIdx = j
			}
		}

		if bestAplicIdx != -1 {
			aplicarMatch(&resultados, matchedPncp, matchedAplic, pncp, bestAplicIdx, "MATCH_PARCIAL", bestTexto, bestValor, bestData, bestDiffValor, bestDiffData, "secundario_cnpj_data")
			matchesTier2++
		}
	}
	log.Printf("[Tier 2] %d matches por CNPJ+Data\n", matchesTier2)

	matchesTier3 := 0
	for _, pncp := range pncpRecords {
		if matchedPncp[pncp.NumeroControlePNCP] {
			continue
		}

		numPncp, anoPncp := extrairNumeroAno(pncp.NumeroCompra)
		valPncp := valorPncpBase(pncp)

		for j, aplic := range aplicRecords {
			if matchedAplic[fmt.Sprintf("%d", j)] {
				continue
			}

			if normalizarTexto(aplic.Municipio) != normalizarTexto(pncp.MunicipioNome) {
				continue
			}
			if aplic.Numero != numPncp || aplic.Ano != anoPncp || aplic.ModalidadePNCPId != pncp.ModalidadeId {
				continue
			}

			texto := SimilaridadeObjeto(normalizarTexto(pncp.ObjetoCompra), normalizarTexto(aplic.Objetivo))
			valor, diffValor := scoreValor(valPncp, aplic.ValorEstimado)
			data, diffData := scoreData(pncp.DataPublicacaoPncp, aplic.DataAbertura)
			aplicarMatch(&resultados, matchedPncp, matchedAplic, pncp, j, "MATCH_PARCIAL", texto, valor, data, diffValor, diffData, "terciario_estrutural")
			matchesTier3++
			break
		}
	}
	log.Printf("[Tier 3] %d matches estruturais\n", matchesTier3)

	for _, pncp := range pncpRecords {
		if !matchedPncp[pncp.NumeroControlePNCP] {
			resultados = append(resultados, MatchResult{
				IdPNCP:           pncp.NumeroControlePNCP,
				IdAPLIC:          "",
				Municipio:        pncp.MunicipioNome,
				StatusCruzamento: "SEM_MATCH",
				ScoreComposto:    0,
				ScoreTexto:       0,
				ScoreValor:       0,
				ScoreData:        0,
				EstrategiaMatch:  "sem_match",
			})
		}
	}

	for j, aplic := range aplicRecords {
		if !matchedAplic[fmt.Sprintf("%d", j)] {
			resultados = append(resultados, MatchResult{
				IdPNCP:           "",
				IdAPLIC:          fmt.Sprintf("APLIC-%d", j),
				Municipio:        aplic.Municipio,
				StatusCruzamento: "APENAS_APLIC",
				ScoreComposto:    0,
				ScoreTexto:       0,
				ScoreValor:       0,
				ScoreData:        0,
				EstrategiaMatch:  "sem_par_pncp",
			})
		}
	}

	if err := SalvarResultados(resultados); err != nil {
		return nil, err
	}

	return resultados, nil
}

func SalvarResultados(resultados []MatchResult) error {
	conn, err := db.GetConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO crossmatch_results (id_pncp, id_aplic, municipio, status_cruzamento, score_composto, estrategia_match) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range resultados {
		pId := r.IdPNCP
		aId := r.IdAPLIC
		if pId == "" {
			pId = "N/A-" + time.Now().String()
		}
		if aId == "" {
			aId = "N/A-" + time.Now().String()
		}

		_, err := stmt.Exec(pId, aId, r.Municipio, r.StatusCruzamento, r.ScoreComposto, r.EstrategiaMatch)
		if err != nil {
			log.Println("Error inserting crossmatch:", err)
		}
	}

	return tx.Commit()
}
