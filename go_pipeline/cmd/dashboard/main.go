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
	"github.com/ericguerrize/pncp-go/internal/models"
	"github.com/ericguerrize/pncp-go/internal/processor"
)

const cacheTTL = 10 * time.Minute

// statewideTTL is longer than cacheTTL: the statewide PNCP fetch (~500 requests covering
// every município in MT) is much more expensive than a single município's crossmatch, so
// it's worth reusing across município requests for longer.
const statewideTTL = 60 * time.Minute

type statewideEntry struct {
	Records  []models.ProcessedCompra
	FetchedAt time.Time
}

var (
	statewideMu    sync.Mutex
	statewideCache = make(map[string]*statewideEntry) // keyed by ano
)

// getStatewideRecords returns every PNCP compra published in MT for anoStr, fetching and
// caching the whole state in one pass so that looking up any município never needs to know
// its entities' CNPJs upfront — the município name comes back on each PNCP record itself.
func getStatewideRecords(anoStr string, force bool, report func(stage, message string, current, total int)) ([]models.ProcessedCompra, error) {
	if !force {
		statewideMu.Lock()
		entry, ok := statewideCache[anoStr]
		statewideMu.Unlock()
		if ok && time.Since(entry.FetchedAt) < statewideTTL {
			report("pncp", fmt.Sprintf("Usando índice estadual em cache (%s, %d registros)...", anoStr, len(entry.Records)), 0, 0)
			return entry.Records, nil
		}
	}

	report("pncp", fmt.Sprintf("Baixando contratações do estado inteiro (MT/%s)...", anoStr), 0, len(config.Modalidades))
	apiClient := api.NewClient()
	col := collector.NewCollector(apiClient)
	dataInicial := anoStr + "0101"
	dataFinal := anoStr + "1231"
	raw := col.CollectAllData(dataInicial, dataFinal, func(done, total int) {
		report("pncp", fmt.Sprintf("Baixando o índice estadual do PNCP... (%d/%d requisições)", done, total), done, total)
	})

	records := processor.NormalizeResults(raw, time.Now())
	log.Printf("[Live API] Índice estadual PNCP (%s): %d registros em %d municípios\n",
		anoStr, len(records), countMunicipios(records))

	statewideMu.Lock()
	statewideCache[anoStr] = &statewideEntry{Records: records, FetchedAt: time.Now()}
	statewideMu.Unlock()

	return records, nil
}

func countMunicipios(records []models.ProcessedCompra) int {
	seen := make(map[string]bool)
	for _, r := range records {
		seen[crossmatch.NormalizeMunicipio(r.MunicipioNome)] = true
	}
	return len(seen)
}

// Job tracks the progress and outcome of one asynchronous crossmatch run so the
// frontend can poll it instead of blocking on a single multi-minute HTTP request.
type Job struct {
	mu       sync.Mutex
	Status   string // running, done, error
	Stage    string
	Message  string
	Current  int
	Total    int
	Result   map[string]interface{}
	Error    string
	CachedAt time.Time
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
	j.Status = "error"
	j.Error = err.Error()
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
	}
	return resp
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
// side comes from the statewide índice (see getStatewideRecords), so it works for any
// município in MT regardless of whether we know its entities' CNPJs — CNPJ is only used as a
// secondary matching signal, never to decide which PNCP data to fetch.
func executeCrossmatch(municipio, anoStr string, force bool, onProgress func(stage, message string, current, total int)) (map[string]interface{}, error) {
	report := func(stage, message string, current, total int) {
		log.Printf("[Live API][%s] %s\n", stage, message)
		if onProgress != nil {
			onProgress(stage, message, current, total)
		}
	}

	report("aplic", fmt.Sprintf("Extraindo licitações do APLIC para %s...", municipio), 0, 0)
	aplicData, err := crossmatch.CarregarAplicOracleAoVivo(municipio, anoStr)
	if err != nil {
		return nil, fmt.Errorf("erro ou nenhuma UG encontrada: %v", err)
	}

	statewide, err := getStatewideRecords(anoStr, force, report)
	if err != nil {
		return nil, fmt.Errorf("erro ao consultar o PNCP: %v", err)
	}

	targetMun := crossmatch.NormalizeMunicipio(municipio)
	var normalizedPncp []models.ProcessedCompra
	for _, p := range statewide {
		if crossmatch.NormalizeMunicipio(p.MunicipioNome) == targetMun {
			normalizedPncp = append(normalizedPncp, p)
		}
	}
	log.Printf("[Live API] Registros PNCP encontrados para %s: %d (de %d no estado)\n", municipio, len(normalizedPncp), len(statewide))

	report("crossmatch", "Conciliando registros do APLIC e do PNCP...", 0, 0)
	matches, _ := crossmatch.ExecutarCruzamento(normalizedPncp, aplicData)

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
