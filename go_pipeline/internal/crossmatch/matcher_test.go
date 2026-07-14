package crossmatch

import "testing"

func TestNormalizeMunicipioRemovesAccentsAndPunctuation(t *testing.T) {
	got := NormalizeMunicipio(" Rondonópolis/MT ")
	want := "rondonopolis mt"
	if got != want {
		t.Fatalf("NormalizeMunicipio() = %q, want %q", got, want)
	}
}

func TestSimilaridadeObjetoIgnoresBoilerplate(t *testing.T) {
	a := "Contratacao de empresa especializada para reforma da unidade basica de saude central"
	b := "Contratacao de empresa especializada para reforma da unidade basica de saude central"
	score := SimilaridadeObjeto(a, b)
	if score < 99 {
		t.Fatalf("expected near exact score, got %.2f", score)
	}
}

func TestSimilaridadeObjetoSeparatesDifferentObjects(t *testing.T) {
	a := "Contratacao de empresa especializada para construcao de creche municipal"
	b := "Contratacao de empresa especializada para locacao de software tributario"
	score := SimilaridadeObjeto(a, b)
	if score >= 30 {
		t.Fatalf("expected low score for unrelated objects, got %.2f", score)
	}
}

func TestScoreHelpers(t *testing.T) {
	valor, diffValor := scoreValor(100, 105)
	if valor <= 0 || diffValor <= 0 {
		t.Fatalf("expected positive value score and diff, got %.2f / %.2f", valor, diffValor)
	}
}
