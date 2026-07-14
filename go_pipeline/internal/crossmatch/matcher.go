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
	IdPNCP           string
	IdAPLIC          string
	Municipio        string
	StatusCruzamento string
	ScoreComposto    float64
	EstrategiaMatch  string
}

// objetoStopwords are connector words and procurement boilerplate that appear in nearly
// every objeto text regardless of what's actually being contracted ("CONTRATAÇÃO DE EMPRESA
// ESPECIALIZADA...DE ACORDO COM OS TERMOS E ESPECIFICAÇÕES DO EDITAL..."). Left in, they
// made character-level string similarity (Jaro-Winkler) score unrelated objects (e.g. a public
// safety center vs. a health clinic) above 85% purely on shared framing — confirmed against
// real Sinop data where that produced false-positive matches. Stripping them and comparing the
// remaining distinctive words directly is far more discriminative for this kind of text.
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

// SimilaridadeObjeto scores two objeto texts by the Jaccard overlap of their distinctive
// (non-boilerplate) words — the fraction of meaningful words they share — rather than raw
// character similarity, so two texts sharing only administrative boilerplate score near zero.
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

	return float64(intersection) / float64(len(union)) * 100.0
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
	return accentReplacer.Replace(strings.ToLower(strings.TrimSpace(s)))
}

var accentReplacer = strings.NewReplacer(
	"á", "a", "à", "a", "ã", "a", "â", "a", "ä", "a",
	"é", "e", "è", "e", "ê", "e", "ë", "e",
	"í", "i", "ì", "i", "î", "i", "ï", "i",
	"ó", "o", "ò", "o", "õ", "o", "ô", "o", "ö", "o",
	"ú", "u", "ù", "u", "û", "u", "ü", "u",
	"ç", "c",
)

// NormalizeMunicipio lowercases, trims and strips accents so that PNCP's
// "Rondonópolis" and Oracle's already-accent-stripped "RONDONOPOLIS" compare equal.
func NormalizeMunicipio(s string) string {
	return accentReplacer.Replace(strings.ToLower(strings.TrimSpace(s)))
}

func ExecutarCruzamento(pncpRecords []models.ProcessedCompra, aplicRecords []AplicData) ([]MatchResult, error) {
	log.Println("Iniciando Crossmatch (Go)...")
	var resultados []MatchResult
	
	matchedPncp := make(map[string]bool)
	matchedAplic := make(map[string]bool)

	// Tier 1 - Semantic + Financial
	for _, pncp := range pncpRecords {
		if matchedPncp[pncp.NumeroControlePNCP] {
			continue
		}
		
		valPncp := pncp.ValorTotalHomologado
		if valPncp == 0 {
			valPncp = pncp.ValorTotalEstimado
		}
		
		objPncpNorm := normalizarTexto(pncp.ObjetoCompra)
		munPncp := normalizarTexto(pncp.MunicipioNome)
		
		var bestScore float64 = 0
		var bestAplicIdx = -1
		
		for j, aplic := range aplicRecords {
			if matchedAplic[fmt.Sprintf("%d", j)] {
				continue
			}
			
			if normalizarTexto(aplic.Municipio) != munPncp || fmt.Sprintf("%d", aplic.Ano) != fmt.Sprintf("%d", pncp.AnoCompra) {
				continue
			}
			
			objAplicNorm := normalizarTexto(aplic.Objetivo)
			score := SimilaridadeObjeto(objPncpNorm, objAplicNorm)

			if score >= 40 {
				// verify financial
				valAplic := aplic.ValorEstimado
				if valPncp > 0 && valAplic > 0 {
					delta := math.Abs(valPncp - valAplic) / math.Max(valPncp, valAplic) * 100
					if delta <= 10 {
						if score > bestScore {
							bestScore = score
							bestAplicIdx = j
						}
					}
				}
			}
		}
		
		if bestAplicIdx != -1 {
			matchedPncp[pncp.NumeroControlePNCP] = true
			matchedAplic[fmt.Sprintf("%d", bestAplicIdx)] = true
			
			resultados = append(resultados, MatchResult{
				IdPNCP:           pncp.NumeroControlePNCP,
				IdAPLIC:          fmt.Sprintf("APLIC-%d", bestAplicIdx), // Generate a unique ID
				Municipio:        pncp.MunicipioNome,
				StatusCruzamento: "MATCH_CONFIRMADO",
				ScoreComposto:    bestScore,
				EstrategiaMatch:  "primario_semantico",
			})
		}
	}
	
	log.Printf("[Tier 1] %d matches semânticos\n", len(resultados))
	
	// Tier 2 - CNPJ + Date
	matchesTier2 := 0
	for _, pncp := range pncpRecords {
		if matchedPncp[pncp.NumeroControlePNCP] {
			continue
		}
		
		cnpjPncp := strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' { return r }
			return -1
		}, pncp.OrgaoEntidadeCNPJ)

		if cnpjPncp == "" {
			continue
		}

		objPncpNorm := normalizarTexto(pncp.ObjetoCompra)

		bestDeltaDays := 9999.0
		bestAplicIdx := -1

		for j, aplic := range aplicRecords {
			if matchedAplic[fmt.Sprintf("%d", j)] {
				continue
			}

			if aplic.CNPJ != cnpjPncp {
				continue
			}

			// CNPJ e data próxima sozinhos não bastam: um mesmo órgão publica várias
			// licitações distintas na mesma janela de dias. Exige-se uma similaridade
			// mínima de objeto (por palavras distintivas, não por caracteres) para
			// descartar pares claramente não relacionados.
			textScore := SimilaridadeObjeto(objPncpNorm, normalizarTexto(aplic.Objetivo))
			if textScore < 30 {
				continue
			}

			// Compare dates if both are valid
			if !pncp.DataPublicacaoPncp.IsZero() && !aplic.DataAbertura.IsZero() {
				delta := math.Abs(pncp.DataPublicacaoPncp.Sub(aplic.DataAbertura).Hours() / 24.0)
				if delta <= 10 && delta < bestDeltaDays {
					bestDeltaDays = delta
					bestAplicIdx = j
				}
			} else if bestAplicIdx == -1 {
				// Sem data em um dos lados: só aceita porque o objeto já passou no filtro de similaridade acima.
				bestDeltaDays = 0
				bestAplicIdx = j
			}
		}
		
		if bestAplicIdx != -1 {
			matchedPncp[pncp.NumeroControlePNCP] = true
			matchedAplic[fmt.Sprintf("%d", bestAplicIdx)] = true
			
			// compute composite score (Tier 2 leans heavily on date, but we use a default passing score of 70)
			resultados = append(resultados, MatchResult{
				IdPNCP:           pncp.NumeroControlePNCP,
				IdAPLIC:          fmt.Sprintf("APLIC-%d", bestAplicIdx),
				Municipio:        pncp.MunicipioNome,
				StatusCruzamento: "MATCH_PARCIAL",
				ScoreComposto:    75.0, 
				EstrategiaMatch:  "secundario_cnpj_data",
			})
			matchesTier2++
		}
	}
	log.Printf("[Tier 2] %d matches por CNPJ+Data\n", matchesTier2)

	// Tier 3 - Structural Fallback
	matchesTier3 := 0
	for _, pncp := range pncpRecords {
		if matchedPncp[pncp.NumeroControlePNCP] {
			continue
		}
		
		numPncp, anoPncp := extrairNumeroAno(pncp.NumeroCompra)
		
		for j, aplic := range aplicRecords {
			if matchedAplic[fmt.Sprintf("%d", j)] {
				continue
			}
			
			if normalizarTexto(aplic.Municipio) == normalizarTexto(pncp.MunicipioNome) &&
			   aplic.Numero == numPncp && 
			   aplic.Ano == anoPncp && 
			   aplic.ModalidadePNCPId == pncp.ModalidadeId {
				
				matchedPncp[pncp.NumeroControlePNCP] = true
				matchedAplic[fmt.Sprintf("%d", j)] = true
				
				resultados = append(resultados, MatchResult{
					IdPNCP:           pncp.NumeroControlePNCP,
					IdAPLIC:          fmt.Sprintf("APLIC-%d", j),
					Municipio:        pncp.MunicipioNome,
					StatusCruzamento: "MATCH_PARCIAL",
					ScoreComposto:    70.0,
					EstrategiaMatch:  "terciario_estrutural",
				})
				matchesTier3++
				break
			}
		}
	}
	log.Printf("[Tier 3] %d matches estruturais\n", matchesTier3)
	
	// Remainder are marked as NO MATCH / ORPHANS
	for _, pncp := range pncpRecords {
		if !matchedPncp[pncp.NumeroControlePNCP] {
			resultados = append(resultados, MatchResult{
				IdPNCP:           pncp.NumeroControlePNCP,
				IdAPLIC:          "",
				Municipio:        pncp.MunicipioNome,
				StatusCruzamento: "SEM_MATCH",
				ScoreComposto:    0,
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
				EstrategiaMatch:  "sem_par_pncp",
			})
		}
	}
	
	// Salvar no banco
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
		// handle empty IDs for uniqueness
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
