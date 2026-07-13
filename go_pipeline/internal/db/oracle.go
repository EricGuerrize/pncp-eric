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

func normalizarParaOracle(texto string) string {
	s := strings.ToUpper(texto)
	// Add proper normalization as needed, removing accents
	return strings.TrimSpace(s)
}

type UGInfo struct {
	UGCode string
	Nome   string
	CNPJ   string
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
			E.CNPJ_DIREITO_PUBLICO AS cnpj_publico
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
		var ug, nome string
		var cnpj sql.NullString
		if err := rows.Scan(&ug, &nome, &cnpj); err == nil {

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
				UGCode: ug,
				Nome: nome,
				CNPJ: cleanCNPJ,
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
