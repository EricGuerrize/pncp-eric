package collector

import (
	"log"
	"sync"

	"github.com/ericguerrize/pncp-go/internal/api"
	"github.com/ericguerrize/pncp-go/internal/config"
	"github.com/ericguerrize/pncp-go/internal/models"
)

type Collector struct {
	client *api.Client
}

func NewCollector(client *api.Client) *Collector {
	return &Collector{
		client: client,
	}
}

type CollectResult struct {
	CNPJ       string
	Modalidade int
	Pagina     int
	Response   *models.PNCPResponse
	Error      error
}

// ProgressFunc reports how many requests have completed out of the current known total.
// The total may grow as additional pages are discovered.
type ProgressFunc func(done, total int)

// CollectAllData fetches all pages across all modalities concurrently for the whole UF
// (no CNPJ filter), so it covers every município in one pass regardless of whether we
// know its entities' CNPJs. onProgress, if non-nil, is invoked after every completed request.
func (c *Collector) CollectAllData(dataInicial, dataFinal string, onProgress ProgressFunc) []CollectResult {
	var allResults []CollectResult
	var mu sync.Mutex

	// Semaphore to limit concurrent requests
	sem := make(chan struct{}, config.MaxConcurrentRequests)
	var wg sync.WaitGroup

	total := len(config.Modalidades)
	done := 0
	report := func() {
		if onProgress == nil {
			return
		}
		mu.Lock()
		done++
		d, t := done, total
		mu.Unlock()
		onProgress(d, t)
	}

	log.Println("Buscando a primeira página de cada modalidade...")

	// First pass: fetch page 1 for all modalities
	var firstPassResults []CollectResult
	for modCode := range config.Modalidades {
		wg.Add(1)
		go func(mod int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			resp, err := c.client.GetContratacoes(dataInicial, dataFinal, mod, 1)

			mu.Lock()
			firstPassResults = append(firstPassResults, CollectResult{
				Modalidade: mod,
				Pagina:     1,
				Response:   resp,
				Error:      err,
			})
			mu.Unlock()
			report()
		}(modCode)
	}

	wg.Wait()

	allResults = append(allResults, firstPassResults...)

	// Second pass: fetch remaining pages
	type pageReq struct {
		ModCode int
		Page    int
	}
	var remainingPages []pageReq

	for _, result := range firstPassResults {
		if result.Error == nil && result.Response != nil && result.Response.TotalPaginas > 1 {
			log.Printf("Modalidade %d (%s) - Total de páginas: %d\n",
				result.Modalidade, config.Modalidades[result.Modalidade], result.Response.TotalPaginas)
			for p := 2; p <= result.Response.TotalPaginas; p++ {
				remainingPages = append(remainingPages, pageReq{ModCode: result.Modalidade, Page: p})
			}
		}
	}

	if len(remainingPages) > 0 {
		mu.Lock()
		total += len(remainingPages)
		mu.Unlock()

		log.Printf("Buscando as %d páginas restantes...\n", len(remainingPages))
		for _, req := range remainingPages {
			wg.Add(1)
			go func(req pageReq) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				resp, err := c.client.GetContratacoes(dataInicial, dataFinal, req.ModCode, req.Page)

				mu.Lock()
				allResults = append(allResults, CollectResult{
					Modalidade: req.ModCode,
					Pagina:     req.Page,
					Response:   resp,
					Error:      err,
				})
				mu.Unlock()
				report()
			}(req)
		}
		wg.Wait()
	}

	return allResults
}

// CollectByCNPJs fetches pages across all modalities concurrently for specific CNPJs.
// onProgress, if non-nil, is invoked (concurrency-safely) after every completed request.
func (c *Collector) CollectByCNPJs(cnpjs []string, dataInicial, dataFinal string, onProgress ProgressFunc) []CollectResult {
	var allResults []CollectResult
	var mu sync.Mutex

	sem := make(chan struct{}, config.MaxConcurrentRequests)
	var wg sync.WaitGroup

	total := len(cnpjs) * len(config.Modalidades)
	done := 0
	report := func() {
		if onProgress == nil {
			return
		}
		mu.Lock()
		done++
		d, t := done, total
		mu.Unlock()
		onProgress(d, t)
	}

	log.Printf("Buscando a primeira página para %d CNPJs...", len(cnpjs))

	var firstPassResults []CollectResult
	for _, cnpj := range cnpjs {
		for modCode := range config.Modalidades {
			wg.Add(1)
			go func(c_cnpj string, mod int) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				resp, err := c.client.GetContratacoesPorCNPJ(dataInicial, dataFinal, c_cnpj, mod, 1)

				mu.Lock()
				firstPassResults = append(firstPassResults, CollectResult{
					CNPJ:       c_cnpj,
					Modalidade: mod,
					Pagina:     1,
					Response:   resp,
					Error:      err,
				})
				mu.Unlock()
				report()
			}(cnpj, modCode)
		}
	}
	wg.Wait()

	allResults = append(allResults, firstPassResults...)

	type pageReq struct {
		CNPJ    string
		ModCode int
		Page    int
	}
	var remainingPages []pageReq

	for _, result := range firstPassResults {
		if result.Error == nil && result.Response != nil && result.Response.TotalPaginas > 1 {
			for p := 2; p <= result.Response.TotalPaginas; p++ {
				remainingPages = append(remainingPages, pageReq{CNPJ: result.CNPJ, ModCode: result.Modalidade, Page: p})
			}
		}
	}

	if len(remainingPages) > 0 {
		mu.Lock()
		total += len(remainingPages)
		mu.Unlock()

		log.Printf("Buscando as %d páginas restantes para CNPJs...\n", len(remainingPages))
		for _, req := range remainingPages {
			wg.Add(1)
			go func(req pageReq) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				resp, err := c.client.GetContratacoesPorCNPJ(dataInicial, dataFinal, req.CNPJ, req.ModCode, req.Page)

				mu.Lock()
				allResults = append(allResults, CollectResult{
					CNPJ:       req.CNPJ,
					Modalidade: req.ModCode,
					Pagina:     req.Page,
					Response:   resp,
					Error:      err,
				})
				mu.Unlock()
				report()
			}(req)
		}
		wg.Wait()
	}

	return allResults
}

// CollectByMunicipio fetches pages across all modalities concurrently for a single município,
// scoped via PNCP's own codigoMunicipioIbge filter. This needs no CNPJ knowledge at all and,
// because it's scoped to one município instead of the whole UF, needs far fewer requests than
// a statewide bulk fetch — the right default for a single município's live crossmatch.
func (c *Collector) CollectByMunicipio(codigoIbge, dataInicial, dataFinal string, onProgress ProgressFunc) []CollectResult {
	var allResults []CollectResult
	var mu sync.Mutex

	sem := make(chan struct{}, config.MaxConcurrentRequests)
	var wg sync.WaitGroup

	total := len(config.Modalidades)
	done := 0
	report := func() {
		if onProgress == nil {
			return
		}
		mu.Lock()
		done++
		d, t := done, total
		mu.Unlock()
		onProgress(d, t)
	}

	log.Printf("Buscando a primeira página de cada modalidade para o município IBGE %s...\n", codigoIbge)

	var firstPassResults []CollectResult
	for modCode := range config.Modalidades {
		wg.Add(1)
		go func(mod int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			resp, err := c.client.GetContratacoesPorMunicipio(dataInicial, dataFinal, codigoIbge, mod, 1)

			mu.Lock()
			firstPassResults = append(firstPassResults, CollectResult{
				Modalidade: mod,
				Pagina:     1,
				Response:   resp,
				Error:      err,
			})
			mu.Unlock()
			report()
		}(modCode)
	}
	wg.Wait()

	allResults = append(allResults, firstPassResults...)

	type pageReq struct {
		ModCode int
		Page    int
	}
	var remainingPages []pageReq

	for _, result := range firstPassResults {
		if result.Error == nil && result.Response != nil && result.Response.TotalPaginas > 1 {
			for p := 2; p <= result.Response.TotalPaginas; p++ {
				remainingPages = append(remainingPages, pageReq{ModCode: result.Modalidade, Page: p})
			}
		}
	}

	if len(remainingPages) > 0 {
		mu.Lock()
		total += len(remainingPages)
		mu.Unlock()

		log.Printf("Buscando as %d páginas restantes para o município IBGE %s...\n", len(remainingPages), codigoIbge)
		for _, req := range remainingPages {
			wg.Add(1)
			go func(req pageReq) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				resp, err := c.client.GetContratacoesPorMunicipio(dataInicial, dataFinal, codigoIbge, req.ModCode, req.Page)

				mu.Lock()
				allResults = append(allResults, CollectResult{
					Modalidade: req.ModCode,
					Pagina:     req.Page,
					Response:   resp,
					Error:      err,
				})
				mu.Unlock()
				report()
			}(req)
		}
		wg.Wait()
	}

	return allResults
}
