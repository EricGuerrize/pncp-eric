package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ericguerrize/pncp-go/internal/api"
	"github.com/ericguerrize/pncp-go/internal/collector"
	"github.com/ericguerrize/pncp-go/internal/config"
	"github.com/ericguerrize/pncp-go/internal/crossmatch"
	"github.com/ericguerrize/pncp-go/internal/db"
	"github.com/ericguerrize/pncp-go/internal/processor"
)

const cacheTTL = 10 * time.Minute

// Job tracks the progress and outcome of one asynchronous crossmatch run so the
// frontend can poll it instead of blocking on a single multi-minute HTTP request.
type Job struct {
	mu        sync.Mutex
	Status    string // running, done, error
	Stage     string
	Message   string
	Current   int
	Total     int
	Result    map[string]interface{}
	Error     string
	ErrorCode string
	ErrorHint string
	CachedAt  time.Time
}

func (j *Job) update(stage, message string, current, total int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Stage = stage
	j.Message = message
	j.Current = current
	j.Total = total
}

func (j *Job) finish(result map[string]interface{}, cachedAt time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Status = "done"
	j.Stage = "done"
	j.Message = "Concluído"
	j.Result = result
	j.CachedAt = cachedAt
}

func (j *Job) fail(err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	code, message, hint := classifyJobError(err)
	j.Status = "error"
	j.Error = err.Error()
	j.ErrorCode = code
	j.ErrorHint = hint
	j.Message = message
}

func (j *Job) snapshot() map[string]interface{} {
	j.mu.Lock()
	defer j.mu.Unlock()
	resp := map[string]interface{}{
		"status":  j.Status,
		"stage":   j.Stage,
		"message": j.Message,
		"current": j.Current,
		"total":   j.Total,
	}
	if j.Status == "done" {
		if inner, ok := j.Result["data"]; ok {
			resp["data"] = inner
		} else {
			resp["data"] = j.Result
		}
		if !j.CachedAt.IsZero() {
			resp["cachedAt"] = j.CachedAt.Format(time.RFC3339)
		}
	}
	if j.Status == "error" {
		resp["error"] = j.Error
		resp["errorCode"] = j.ErrorCode
		resp["errorHint"] = j.ErrorHint
	}
	return resp
}

func classifyJobError(err error) (code, message, hint string) {
	raw := strings.ToLower(err.Error())

	switch {
	case strings.Contains(raw, "no such host"), strings.Contains(raw, "lookup "), strings.Contains(raw, "dial tcp"):
		return "oracle_dns_unavailable",
			"Nao foi possivel localizar o servidor Oracle do TCE.",
			"Verifique VPN, DNS ou acesso a rede interna antes de tentar novamente."
	case strings.Contains(raw, "timeout"), strings.Contains(raw, "i/o timeout"):
		return "oracle_timeout",
			"A conexao com o Oracle demorou mais do que o esperado.",
			"Tente novamente em alguns minutos ou valide a conectividade com o banco."
	case strings.Contains(raw, "nenhuma ug encontrada"):
		return "oracle_no_ug",
			"Nenhuma UG foi encontrada para o municipio informado.",
			"Confira o nome do municipio e se ele existe na base Oracle consultada."
	default:
		return "crossmatch_failed",
			"O cruzamento nao pode ser concluido.",
			"Consulte os logs da API para ver o erro tecnico completo."
	}
}

type cacheEntry struct {
	Result   map[string]interface{}
	CachedAt time.Time
}

var (
	jobsMu sync.Mutex
	jobs   = make(map[string]*Job)

	cacheMu sync.Mutex
	cache   = make(map[string]*cacheEntry)
)

func newJobID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func cacheKey(municipio, ano string) string {
	return strings.ToLower(strings.TrimSpace(municipio)) + "|" + ano
}

// executeCrossmatch runs the full Oracle (APLIC) + PNCP crossmatch for a município/ano,
// reporting progress through onProgress (which may be nil for synchronous callers). The PNCP
// side is fetched scoped directly to the município via PNCP's own codigoMunicipioIbge filter
// (derived from Oracle's MUN_CODIGO, see db.MunCodigoToIbge), so it works for any município in
// MT without needing to know any of its entities' CNPJs upfront — CNPJ is only used as a
// secondary matching signal (Tier 2), never to decide which PNCP data to fetch. force is
// accepted for API symmetry with the job cache in main(); the PNCP fetch itself is never cached
// beyond that.
func executeCrossmatch(municipio, anoStr string, force bool, onProgress func(stage, message string, current, total int)) (map[string]interface{}, error) {
	report := func(stage, message string, current, total int) {
		log.Printf("[Live API][%s] %s\n", stage, message)
		if onProgress != nil {
			onProgress(stage, message, current, total)
		}
	}

	report("ugs", fmt.Sprintf("Descobrindo UGs no Oracle TCE para %s...", municipio), 0, 0)
	ugsInfo, err := db.DescobrirUGs(municipio)
	if err != nil || len(ugsInfo) == 0 {
		return nil, fmt.Errorf("erro ou nenhuma UG encontrada: %v", err)
	}
	codigoIbge := ugsInfo[0].CodigoIbge
	if codigoIbge == "" {
		return nil, fmt.Errorf("não foi possível determinar o código IBGE do município %s", municipio)
	}

	report("aplic", fmt.Sprintf("Extraindo licitações do APLIC para %s...", municipio), 0, 0)
	aplicData, err := crossmatch.CarregarAplicOracleAoVivo(municipio, anoStr)
	if err != nil {
		return nil, fmt.Errorf("erro ao extrair APLIC: %v", err)
	}

	report("pncp", fmt.Sprintf("Consultando o PNCP para %s (IBGE %s)...", municipio, codigoIbge), 0, len(config.Modalidades))
	apiClient := api.NewClient()
	col := collector.NewCollector(apiClient)
	dataInicial := anoStr + "0101"
	dataFinal := anoStr + "1231"
	rawResults := col.CollectByMunicipio(codigoIbge, dataInicial, dataFinal, func(done, total int) {
		report("pncp", fmt.Sprintf("Consultando o PNCP... (%d/%d requisições)", done, total), done, total)
	})

	normalizedPncp := processor.NormalizeResults(rawResults, time.Now())
	log.Printf("[Live API] Registros PNCP encontrados para %s: %d\n", municipio, len(normalizedPncp))

	report("crossmatch", "Conciliando registros do APLIC e do PNCP...", 0, 0)
	matches, err := crossmatch.ExecutarCruzamento(normalizedPncp, aplicData)
	if err != nil {
		return nil, fmt.Errorf("erro ao cruzar registros: %v", err)
	}

	var ambos, apenasPncp []map[string]interface{}
	matchMap := make(map[string]crossmatch.MatchResult)
	for _, m := range matches {
		matchMap[m.IdPNCP] = m
	}

	for _, p := range normalizedPncp {
		anoPncp := strconv.Itoa(p.AnoCompra)

		record := map[string]interface{}{
			"orgao":              p.NomeUnidade,
			"modalidade":         p.ModalidadeNome,
			"numero":             p.NumeroCompra,
			"ano":                anoPncp,
			"objeto":             p.ObjetoCompra,
			"valor":              p.ValorTotalHomologado,
			"cnpj":               p.OrgaoEntidadeCNPJ,
			"numeroControlePNCP": p.NumeroControlePNCP,
			"dataPublicacaoPncp": p.DataPublicacaoPncp.Format("2006-01-02"),
		}

		if record["valor"] == 0.0 {
			record["valor"] = p.ValorTotalEstimado
		}

		m, exists := matchMap[p.NumeroControlePNCP]
		if exists && (m.StatusCruzamento == "MATCH_CONFIRMADO" || m.StatusCruzamento == "MATCH_PARCIAL") {
			record["statusPNCP"] = "S"
			record["statusAPLIC"] = "S"
			record["match_score"] = m.ScoreComposto
			record["score_texto"] = m.ScoreTexto
			record["score_valor"] = m.ScoreValor
			record["score_data"] = m.ScoreData
			record["diff_valor_percent"] = m.DiferencaValor
			record["diff_data_dias"] = m.DiferencaDataDias
			record["estrategia"] = m.EstrategiaMatch

			// Expõe o lado APLIC do match para permitir auditar a procedência do cruzamento.
			var aIdx int
			if _, err := fmt.Sscanf(m.IdAPLIC, "APLIC-%d", &aIdx); err == nil && aIdx >= 0 && aIdx < len(aplicData) {
				a := aplicData[aIdx]
				record["orgao_aplic"] = a.OrgaoNome
				record["numero_aplic"] = fmt.Sprintf("%s/%d", a.Numero, a.Ano)
				record["objeto_aplic"] = a.Objetivo
				record["valor_aplic"] = a.ValorEstimado
				if !a.DataAbertura.IsZero() {
					record["dataAPLIC"] = a.DataAbertura.Format("2006-01-02")
				}
			}

			ambos = append(ambos, record)
		} else {
			record["statusPNCP"] = "S"
			record["statusAPLIC"] = "pendente"
			apenasPncp = append(apenasPncp, record)
		}
	}

	var apenasAplic []map[string]interface{}
	for _, m := range matches {
		if m.StatusCruzamento != "APENAS_APLIC" {
			continue
		}

		var idx int
		if _, err := fmt.Sscanf(m.IdAPLIC, "APLIC-%d", &idx); err != nil || idx < 0 || idx >= len(aplicData) {
			continue
		}
		a := aplicData[idx]

		var dataAplic string
		if !a.DataAbertura.IsZero() {
			dataAplic = a.DataAbertura.Format("2006-01-02")
		}

		apenasAplic = append(apenasAplic, map[string]interface{}{
			"orgao":       a.OrgaoNome,
			"modalidade":  a.Modalidade,
			"numero":      a.Numero,
			"ano":         strconv.Itoa(a.Ano),
			"objeto":      a.Objetivo,
			"valor":       a.ValorEstimado,
			"cnpj":        a.CNPJ,
			"dataAPLIC":   dataAplic,
			"statusAPLIC": "S",
			"statusPNCP":  "N",
			"match_score": m.ScoreComposto,
			"score_texto": m.ScoreTexto,
			"score_valor": m.ScoreValor,
			"score_data":  m.ScoreData,
			"estrategia":  m.EstrategiaMatch,
		})
	}

	return map[string]interface{}{
		"status":    "success",
		"municipio": municipio,
		"ano":       anoStr,
		"data": map[string]interface{}{
			"ambos":        ambos,
			"apenas_pncp":  apenasPncp,
			"apenas_aplic": apenasAplic,
		},
	}, nil
}

func main() {
	if err := db.InicializarBanco(); err != nil {
		log.Printf("Falha ao inicializar o banco de dados: %v", err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(config.BaseDir, "..", "dashboard", "live_dashboard.html"))
	})

	http.HandleFunc("/legenda.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(config.BaseDir, "..", "dashboard", "legenda.js"))
	})

	// Starts (or reuses a cached) crossmatch job and returns immediately with a jobId to poll.
	http.HandleFunc("/api/live-crossmatch/start", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		municipio := r.URL.Query().Get("municipio")
		anoStr := r.URL.Query().Get("ano")
		force := r.URL.Query().Get("force") == "true"
		if municipio == "" {
			http.Error(w, `{"status":"error","message":"Parâmetro 'municipio' é obrigatório."}`, http.StatusBadRequest)
			return
		}
		if anoStr == "" {
			anoStr = "2026"
		}

		key := cacheKey(municipio, anoStr)
		job := &Job{Status: "running", Stage: "ugs", Message: "Iniciando..."}
		id := newJobID()
		jobsMu.Lock()
		jobs[id] = job
		jobsMu.Unlock()

		if !force {
			cacheMu.Lock()
			entry, ok := cache[key]
			cacheMu.Unlock()
			if ok && time.Since(entry.CachedAt) < cacheTTL {
				job.finish(entry.Result, entry.CachedAt)
				json.NewEncoder(w).Encode(map[string]interface{}{"jobId": id, "cached": true})
				return
			}
		}

		log.Printf("[Live API] Cruzamento solicitado para: %s (ano: %s, force=%v)\n", municipio, anoStr, force)
		go func() {
			result, err := executeCrossmatch(municipio, anoStr, force, job.update)
			if err != nil {
				job.fail(err)
				return
			}
			now := time.Now()
			cacheMu.Lock()
			cache[key] = &cacheEntry{Result: result, CachedAt: now}
			cacheMu.Unlock()
			job.finish(result, time.Time{})
		}()

		json.NewEncoder(w).Encode(map[string]interface{}{"jobId": id, "cached": false})
	})

	// Polled by the frontend while a job is running to update the progress UI.
	http.HandleFunc("/api/live-crossmatch/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		id := r.URL.Query().Get("jobId")
		jobsMu.Lock()
		job, ok := jobs[id]
		jobsMu.Unlock()
		if !ok {
			http.Error(w, `{"status":"error","message":"Job não encontrado."}`, http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(job.snapshot())
	})

	// Kept for scripting/backwards compatibility: blocks until the crossmatch finishes.
	http.HandleFunc("/api/live-crossmatch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		municipio := r.URL.Query().Get("municipio")
		anoStr := r.URL.Query().Get("ano")
		if municipio == "" {
			http.Error(w, `{"status":"error","message":"Parâmetro 'municipio' é obrigatório."}`, http.StatusBadRequest)
			return
		}
		if anoStr == "" {
			anoStr = "2026"
		}

		result, err := executeCrossmatch(municipio, anoStr, false, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"status":"error","message":"%v"}`, err), http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(result)
	})

	fmt.Println("==================================================")
	fmt.Println("🚀 Servidor API Live PNCP-Go rodando na porta 5000!")
	fmt.Println("Acesse: http://localhost:5000")
	fmt.Println("==================================================")
	log.Fatal(http.ListenAndServe(":5000", nil))
}
