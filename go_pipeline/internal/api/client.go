package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ericguerrize/pncp-go/internal/config"
	"github.com/ericguerrize/pncp-go/internal/models"
)

type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: time.Duration(config.RequestTimeoutSeconds) * time.Second,
		},
	}
}

// pncpRateLimiter paces every outgoing request (regardless of how many goroutines are
// waiting) so we stay under PNCP's undocumented rate limit. Empirically, hitting the API
// with more than ~1 request/second — even sequentially, with no concurrency — starts
// returning HTTP 429 within the first 10 requests.
var pncpRateLimiter = time.NewTicker(1100 * time.Millisecond)

func throttle() {
	<-pncpRateLimiter.C
}

// doRequest performs the GET with retries shared by both PNCP query variants. A 429
// (rate limited) gets a much longer cooldown than other transient failures, since the
// generic short backoff isn't enough for the limit window to clear.
func (c *Client) doRequest(url string) (*models.PNCPResponse, error) {
	const maxRetries = 8

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		throttle()

		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNoContent {
			resp.Body.Close()
			return &models.PNCPResponse{}, nil
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			lastErr = fmt.Errorf("status code 429: rate limited")
			time.Sleep(15 * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("status code %d: %s", resp.StatusCode, string(bodyBytes))
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		var result models.PNCPResponse
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		return &result, nil
	}

	return nil, fmt.Errorf("max retries exceeded for url %s: %w", url, lastErr)
}

// GetContratacoes fetches the PNCP paginated response for a specific modality and page.
func (c *Client) GetContratacoes(dataInicial, dataFinal string, codModalidade, pagina int) (*models.PNCPResponse, error) {
	url := fmt.Sprintf("%s%s?dataInicial=%s&dataFinal=%s&codigoModalidadeContratacao=%d&pagina=%d&tamanhoPagina=%d&uf=%s",
		config.PNCPBaseURL, config.ContratacoesEndpoint,
		dataInicial, dataFinal, codModalidade, pagina, config.TamanhoPagina, config.UF)
	return c.doRequest(url)
}

// GetContratacoesPorCNPJ fetches PNCP paginated response for a specific CNPJ, modality, and date range.
func (c *Client) GetContratacoesPorCNPJ(dataInicial, dataFinal, cnpj string, codModalidade, pagina int) (*models.PNCPResponse, error) {
	url := fmt.Sprintf("%s%s?dataInicial=%s&dataFinal=%s&codigoModalidadeContratacao=%d&pagina=%d&tamanhoPagina=%d&cnpj=%s",
		config.PNCPBaseURL, config.ContratacoesEndpoint,
		dataInicial, dataFinal, codModalidade, pagina, config.TamanhoPagina, cnpj)
	return c.doRequest(url)
}
