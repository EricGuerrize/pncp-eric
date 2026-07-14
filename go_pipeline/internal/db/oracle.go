package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ericguerrize/pncp-go/internal/config"
	"github.com/ericguerrize/pncp-go/internal/orgaos"
	"github.com/joho/godotenv"
	_ "github.com/sijms/go-ora/v2"
)

func getOracleConn() (*sql.DB, error) {
	envPath := filepath.Join(config.BaseDir, "..", ".env")
	godotenv.Load(envPath)

	user := os.Getenv("ORACLE_USER")
	pass := os.Getenv("ORACLE_PASSWORD")
	dsn := os.Getenv("ORACLE_DSN")

	if user == "" || pass == "" || dsn == "" {
		return nil, fmt.Errorf("credenciais Oracle ausentes no .env")
	}

	// format: oracle://user:pass@dsn
	connStr := fmt.Sprintf("oracle://%s:%s@%s", user, pass, dsn)
	return sql.Open("oracle", connStr)
}

var accentReplacer = strings.NewReplacer(
	"Á", "A", "À", "A", "Ã", "A", "Â", "A", "Ä", "A",
	"É", "E", "È", "E", "Ê", "E", "Ë", "E",
	"Í", "I", "Ì", "I", "Î", "I", "Ï", "I",
	"Ó", "O", "Ò", "O", "Õ", "O", "Ô", "O", "Ö", "O",
	"Ú", "U", "Ù", "U", "Û", "U", "Ü", "U",
	"Ç", "C",
)

// normalizarParaOracle uppercases and strips accents so the search term matches the
// TRANSLATE(...)-stripped MUN_NOME column below — otherwise a query for "Cuiabá" (typed
// with the accent) never matches the accent-stripped "CUIABA" stored/compared in Oracle.
func normalizarParaOracle(texto string) string {
	s := accentReplacer.Replace(strings.ToUpper(texto))
	return strings.TrimSpace(s)
}

type UGInfo struct {
	UGCode      string
	Nome        string
	CNPJ        string
	CodigoIbge  string // 7-digit IBGE município code, derived from Oracle's 6-digit MUN_CODIGO
}

// ibgeCheckDigit computes the 7th (verification) digit of a Brazilian município IBGE code
// from its 6-digit base, using the standard alternating 1-2 weighted mod-10 algorithm.
// e.g. Sinop's Oracle MUN_CODIGO "510790" -> check digit "9" -> full IBGE code "5107909".
func ibgeCheckDigit(base string) (string, bool) {
	if len(base) != 6 {
		return "", false
	}
	sum := 0
	weights := [6]int{1, 2, 1, 2, 1, 2}
	for i, r := range base {
		if r < '0' || r > '9' {
			return "", false
		}
		d := int(r - '0')
		p := d * weights[i]
		if p >= 10 {
			p = p/10 + p%10
		}
		sum += p
	}
	check := (10 - (sum % 10)) % 10
	return fmt.Sprintf("%d", check), true
}

// MunCodigoToIbge converts Oracle's 6-digit MUN_CODIGO into the full 7-digit IBGE
// município code expected by PNCP's codigoMunicipioIbge query parameter.
func MunCodigoToIbge(munCodigo string) string {
	munCodigo = strings.TrimSpace(munCodigo)
	check, ok := ibgeCheckDigit(munCodigo)
	if !ok {
		return ""
	}
	return munCodigo + check
}

func DescobrirUGs(municipio string) ([]UGInfo, error) {
	db, err := getOracleConn()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	query := `
		SELECT DISTINCT
			TRIM(TO_CHAR(E.CNPJ_CPF_COD_TCE_ENTIDADE)) AS ug_code,
			E.NOME_entidade AS nome,
			E.CNPJ_DIREITO_PUBLICO AS cnpj_publico,
			TRIM(TO_CHAR(E.MUN_CODIGO)) AS mun_codigo
		FROM aplic2008.ENTIDADE@conectprod E
		JOIN publico.MUNICIPIO@conectprod MN ON MN.MUN_CODIGO = E.MUN_CODIGO
		WHERE TRANSLATE(UPPER(MN.MUN_NOME),
			  'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
			  'AAAAAEEEEIIIIOOOOOOUUUUC') LIKE :1
	`

	busca := fmt.Sprintf("%%%s%%", normalizarParaOracle(municipio))
	rows, err := db.Query(query, busca)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ugs []UGInfo
	for rows.Next() {
		var ug, nome, munCodigo string
		var cnpj sql.NullString
		if err := rows.Scan(&ug, &nome, &cnpj, &munCodigo); err == nil {

			// Clean CNPJ to digits only
			cleanCNPJ := strings.Map(func(r rune) rune {
				if r >= '0' && r <= '9' { return r }
				return -1
			}, cnpj.String)

			if len(cleanCNPJ) < 14 {
				// Oracle não retornou CNPJ_DIREITO_PUBLICO utilizável; tenta o cache local
				if fallback := orgaos.CNPJPorUG(ug); fallback != "" {
					cleanCNPJ = fallback
				} else if fallback := orgaos.CNPJPorMunicipioNome(municipio, nome); fallback != "" {
					cleanCNPJ = fallback
				}
			}

			ugs = append(ugs, UGInfo{
				UGCode:     ug,
				Nome:       nome,
				CNPJ:       cleanCNPJ,
				CodigoIbge: MunCodigoToIbge(munCodigo),
			})
		}
	}
	return ugs, nil
}

// Extracted AplicData struct should ideally live in models or crossmatch. We'll use a local struct for now to avoid circular deps if crossmatch imports db.
// Actually, it's better to just return an array of maps or create a shared model. 
// For now, let's just create the mega query execution.

func ExtrairAplicAoVivo(ugs []string, ano string) ([]map[string]interface{}, error) {
	if len(ugs) == 0 {
		return nil, fmt.Errorf("nenhuma UG fornecida")
	}

	db, err := getOracleConn()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Validate UGs to prevent injection
	var validUgs []string
	for _, ug := range ugs {
		if matched, _ := regexp.MatchString(`^\d+$`, strings.TrimSpace(ug)); matched {
			validUgs = append(validUgs, fmt.Sprintf("'%s'", strings.TrimSpace(ug)))
		}
	}
	if len(validUgs) == 0 {
		return nil, fmt.Errorf("nenhuma UG válida encontrada após limpeza")
	}
	
	ugsStr := strings.Join(validUgs, ", ")

	// BLOCO 1 E BLOCO 2 TRADUZIDOS DA VERSÃO PYTHON
	query := fmt.Sprintf(`
	SELECT *
	FROM (
		SELECT DISTINCT
			   VW.MUN_CODIGO AS MUN_CODIGO,
			   P.ENT_CODIGO AS ENT_CODIGO,
			   TRANSLATE(MN.MUN_NOME, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCaeiouaeiouaeiouaoaeiouc') AS MUNICIPIO,
			   P.PLIC_NUMERO AS NUMERO,
			   PA.PLIC_DATA AS DATA_ABERTURA,
			   P.MLIC_CODIGO AS COD_MODALIDADE,
			   PA.PLIC_VALORESTIMADO AS VALOR_ESTIMADO,
			   TRANSLATE(PA.PLIC_OBJETO, 'ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜÇáéíóúàèìòùâêîôûãõäëïöüç', 'AEIOUAEIOUAEIOUAOAEIOUCaeiouaeiouaeiouaoaeiouc') AS OBJETIVO
		FROM aplic2008.PROCESSO_LICITATORIO@conectprod P
		INNER JOIN aplic2008.ENTIDADE@conectprod VW ON P.ENT_CODIGO = VW.CNPJ_CPF_COD_TCE_ENTIDADE
		INNER JOIN publico.MUNICIPIO@conectprod MN ON MN.MUN_CODIGO = VW.MUN_CODIGO
		INNER JOIN aplic2008.PROC_LICIT_ABERTURA_RETIFIC@conectprod PA 
			ON P.ENT_CODIGO = PA.ENT_CODIGO AND P.PLIC_NUMERO = PA.PLIC_NUMERO AND P.MLIC_CODIGO = PA.MLIC_CODIGO
		WHERE P.ENT_CODIGO IN (%s)
		  AND SUBSTR(P.PLIC_NUMERO, 13, 4) = '%s'
		  AND P.MLIC_CODIGO NOT IN ('17', '22', '23', '25')
	)
	`, ugsStr, ano)

	// We use a simplified version of the giant query here focusing on what crossmatch actually needs.

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	
	var results []map[string]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		
		if err := rows.Scan(columnPointers...); err != nil {
			continue
		}
		
		rowMap := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			rowMap[colName] = *val
		}
		results = append(results, rowMap)
	}

	return results, nil
}
