package orgaos

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ericguerrize/pncp-go/internal/config"
)

type Orgao struct {
	UG        string `json:"ug"`
	Municipio string `json:"municipio"`
	CNPJ      string `json:"cnpj"`
	Nome      string `json:"nome"`
}

var (
	cache     []Orgao
	loadOnce  sync.Once
	byUG      map[string]string
	byMunNome map[string]string
)

func normalizar(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func load() {
	loadOnce.Do(func() {
		path := filepath.Join(config.InputDir, "orgaos.json")
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("[orgaos] não foi possível carregar %s: %v\n", path, err)
			return
		}

		if err := json.Unmarshal(data, &cache); err != nil {
			log.Printf("[orgaos] erro ao decodificar %s: %v\n", path, err)
			return
		}

		byUG = make(map[string]string)
		byMunNome = make(map[string]string)
		for _, o := range cache {
			if o.CNPJ == "" {
				continue
			}
			if o.UG != "" {
				byUG[o.UG] = o.CNPJ
			}
			key := normalizar(o.Municipio) + "|" + normalizar(o.Nome)
			byMunNome[key] = o.CNPJ
		}
		log.Printf("[orgaos] Carregados %d órgãos de %s\n", len(cache), path)
	})
}

// CNPJPorUG retorna o CNPJ cacheado para um código de UG, se existir.
func CNPJPorUG(ug string) string {
	load()
	return byUG[ug]
}

// CNPJPorMunicipioNome tenta achar o CNPJ por município + nome do órgão como fallback.
func CNPJPorMunicipioNome(municipio, nome string) string {
	load()
	key := normalizar(municipio) + "|" + normalizar(nome)
	return byMunNome[key]
}
