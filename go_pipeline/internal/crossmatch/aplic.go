package crossmatch

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	
	"github.com/ericguerrize/pncp-go/internal/config"
	"github.com/ericguerrize/pncp-go/internal/db"
)

type AplicData struct {
	CodUG            string
	Municipio        string
	CNPJ             string
	OrgaoNome        string
	Modalidade       string
	ModalidadeCod    string
	ModalidadePNCPId int
	Numero           string
	Ano              int
	Objetivo         string
	ValorEstimado    float64
	DataAbertura     time.Time
}

func parseValor(valStr string) float64 {
	// Simple BR to float parser: "1.234,56" -> 1234.56
	if valStr == "" {
		return 0.0
	}
	// Remove periods (thousands separators)
	clean := strings.ReplaceAll(valStr, ".", "")
	// Replace comma with period (decimal separator)
	clean = strings.ReplaceAll(clean, ",", ".")
	
	// Remove any remaining non-numeric chars except dot
	var b strings.Builder
	for _, ch := range clean {
		if (ch >= '0' && ch <= '9') || ch == '.' {
			b.WriteRune(ch)
		}
	}
	clean = b.String()

	val, _ := strconv.ParseFloat(clean, 64)
	return val
}

func extrairNumeroAno(texto string) (string, int) {
	// Try to find format like 11/2026 or 011/2026/PMC
	parts := strings.Split(texto, "/")
	if len(parts) >= 2 {
		numero := strings.TrimSpace(parts[0])
		anoStr := strings.TrimSpace(parts[1])
		if len(anoStr) > 4 {
			anoStr = anoStr[:4]
		}
		ano, err := strconv.Atoi(anoStr)
		if err == nil {
			if ano < 100 {
				ano += 2000
			}
			return numero, ano
		}
	}
	
	// If no slash, maybe just a number
	clean := ""
	for _, ch := range texto {
		if ch >= '0' && ch <= '9' {
			clean += string(ch)
		}
	}
	return clean, 0
}

func CarregarAplicCSV(filePath string) ([]AplicData, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	// reader.Comma = ',' // Comma is default
	reader.LazyQuotes = true

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read headers: %w", err)
	}

	headerMap := make(map[string]int)
	for i, h := range headers {
		headerMap[strings.ToLower(strings.TrimSpace(h))] = i
	}

	var results []AplicData

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Warning: error reading APLIC row: %v\n", err)
			continue
		}

		safeGet := func(keys ...string) string {
			for _, k := range keys {
				for hk, idx := range headerMap {
					if strings.Contains(hk, k) && idx < len(record) {
						return strings.TrimSpace(record[idx])
					}
				}
			}
			return ""
		}

		numeroRaw := safeGet("licitaç", "licitac", "nº", "numero")
		exercicioRaw := safeGet("exerc")

		numero, ano := extrairNumeroAno(numeroRaw)
		if ano == 0 && exercicioRaw != "" {
			a, _ := strconv.Atoi(exercicioRaw)
			ano = a
		}

		valorRaw := safeGet("estimado", "valor")
		valor := parseValor(valorRaw)

		dataRaw := safeGet("abertura")
		var data time.Time
		if dataRaw != "" {
			// Assuming DD/MM/YYYY
			d, err := time.Parse("02/01/2006", dataRaw)
			if err == nil {
				data = d
			}
		}

		codMod := safeGet("cod. modalidade", "modalidade_cod")
		
		// Simplify mapping APLIC modality to PNCP directly in code
		pncpModId := 0
		switch codMod {
		case "08", "41": pncpModId = 8
		case "09": pncpModId = 9
		case "13": pncpModId = 6
		case "15": pncpModId = 12
		case "56", "55": pncpModId = 5
		case "01": pncpModId = 4
		case "02": pncpModId = 2
		case "03": pncpModId = 3
		case "04": pncpModId = 1
		case "05": pncpModId = 7
		case "06": pncpModId = 10
		case "07": pncpModId = 11
		case "14": pncpModId = 13
		}

		obj := safeGet("objetivo", "objeto")
		if obj == "" {
			obj = safeGet("motivo")
		}

		item := AplicData{
			CodUG:            safeGet("ug"),
			Municipio:        safeGet("município", "municipio"),
			Modalidade:       safeGet("modalidade"),
			ModalidadeCod:    codMod,
			ModalidadePNCPId: pncpModId,
			Numero:           numero,
			Ano:              ano,
			Objetivo:         obj,
			ValorEstimado:    valor,
			DataAbertura:     data,
		}
		
		results = append(results, item)
	}

	log.Printf("Carregados %d registros do arquivo APLIC.\n", len(results))
	return results, nil
}

// toFloat64 converts values coming back from the Oracle driver (which may surface
// NUMBER columns as float64, int64, []byte or string depending on the value) into a float64.
func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case []byte:
		f, _ := strconv.ParseFloat(strings.TrimSpace(string(n)), 64)
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f
	default:
		return 0
	}
}

func CarregarAplicOracleAoVivo(municipio string, ano string) ([]AplicData, error) {
	ugsInfo, err := db.DescobrirUGs(municipio)
	if err != nil {
		return nil, fmt.Errorf("erro descobrindo UGs no Oracle: %v", err)
	}

	if len(ugsInfo) == 0 {
		return nil, fmt.Errorf("nenhuma UG encontrada no Oracle para o município %s", municipio)
	}

	var ugs []string
	ugCnpj := make(map[string]string)
	ugNome := make(map[string]string)
	for _, info := range ugsInfo {
		ugs = append(ugs, info.UGCode)
		if info.CNPJ != "" {
			ugCnpj[info.UGCode] = info.CNPJ
		}
		ugNome[info.UGCode] = info.Nome
	}

	log.Printf("Buscando dados APLIC ao vivo no Oracle TCE para %d UGs de %s", len(ugs), municipio)
	rawRecords, err := db.ExtrairAplicAoVivo(ugs, ano)
	if err != nil {
		return nil, fmt.Errorf("erro extraindo dados APLIC: %v", err)
	}

	var records []AplicData
	for _, row := range rawRecords {
		// As map keys might be capitalized depending on the DB driver return format
		
		var num, obj, mun, mod string
		var data time.Time
		var val float64
		var anoInt int
		
		fmt.Sscanf(ano, "%d", &anoInt)

		if v, ok := row["NUMERO"].(string); ok { num = v }
		if v, ok := row["OBJETIVO"].(string); ok { obj = v }
		if v, ok := row["MUNICIPIO"].(string); ok { mun = v }
		if v, ok := row["COD_MODALIDADE"].(string); ok { mod = v }

		if v, ok := row["DATA_ABERTURA"].(time.Time); ok { data = v }
		val = toFloat64(row["VALOR_ESTIMADO"])

		entCodigo := fmt.Sprintf("%v", row["ENT_CODIGO"])
		cnpj := ugCnpj[entCodigo]
		orgaoNome := ugNome[entCodigo]

		// Adjust modalidade mapped
		modMap := map[string]string{
			"09": "9", "08": "8", "04": "4", "06": "6", "07": "7", "12": "12",
		}
		modIdStr := modMap[mod]
		if modIdStr == "" {
			modIdStr = mod
		}
		modId, _ := strconv.Atoi(modIdStr)

		records = append(records, AplicData{
			CodUG: entCodigo,
			OrgaoNome: orgaoNome,
			Numero: num,
			Ano: anoInt,
			Objetivo: obj,
			Municipio: mun,
			Modalidade: config.Modalidades[modId],
			ModalidadeCod: mod,
			DataAbertura: data,
			ValorEstimado: val,
			ModalidadePNCPId: modId,
			CNPJ: cnpj,
		})
	}

	log.Printf("Carregados %d registros APLIC ao vivo do Oracle TCE-MT.", len(records))
	return records, nil
}
