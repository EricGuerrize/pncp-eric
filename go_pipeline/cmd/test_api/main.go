package main
import (
	"fmt"
	"github.com/ericguerrize/pncp-go/internal/api"
)
func main() {
	client := api.NewClient()
	resp, err := client.GetContratacoes("20260101", "20260325", 6, 1)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Total Registros:", resp.TotalRegistros)
	if len(resp.Data) > 0 {
		fmt.Println("First item sphere:", resp.Data[0].OrgaoEntidade.EsferaId)
	}
}
