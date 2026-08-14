// Package units parseia os valores escalares que aparecem nos dois YAMLs do
// produto: tamanhos em bytes, durações e inteiros positivos.
//
// Vive num pacote próprio porque os dois arquivos precisam da MESMA regra. O
// nishidb.yaml (spec §78) já usava estes parsers; o YAML do dataset (§12)
// passou a precisar deles no M2, em source.kafka.consumer. Duplicar a
// gramática criaria uma segunda fonte da verdade, e o dia em que as duas
// divergissem "512MiB" significaria coisas diferentes em arquivos diferentes.
//
// O contrato das três funções é o mesmo: ausente herda o padrão, presente e
// inválido é ERRO. Nenhuma delas cai no padrão em silêncio quando o operador
// escreveu algo — cair no padrão é o que faz a configuração mentir.
package units

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// InteiroPositivo aplica o padrão quando a chave está AUSENTE e recusa
// qualquer valor não positivo. Zero e negativo não são "sem valor": são teto
// que nenhuma operação poderia satisfazer, e aceitá-los em silêncio faria a
// configuração mentir.
//
// O ponteiro é o que separa "ausente" de "zero": sem ele, o YAML que declara
// `maxRows: 0` seria indistinguível do que não declara nada.
func InteiroPositivo(v *int, padrao int) (int, error) {
	if v == nil {
		return padrao, nil
	}
	if *v <= 0 {
		return 0, fmt.Errorf("deve ser positivo, é %d", *v)
	}
	return *v, nil
}

// Tamanho aceita as unidades binárias da spec (512MiB, 1GiB) e o número puro
// em bytes. As unidades DECIMAIS (MB, GB) são recusadas em vez de
// interpretadas: aceitar as duas grafias faria "512MB" e "512MiB" parecerem a
// mesma coisa e diferirem em 7% de disco.
func Tamanho(s string, padrao int64) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return padrao, nil
	}
	unidades := []struct {
		sufixo string
		mult   int64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}, {"B", 1},
	}
	for _, u := range unidades {
		if !strings.HasSuffix(s, u.sufixo) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(s, u.sufixo)), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("valor inválido %q", s)
		}
		if n <= 0 {
			return 0, fmt.Errorf("valor deve ser positivo, é %q", s)
		}
		return n * u.mult, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("valor inválido %q (use bytes ou sufixo KiB/MiB/GiB)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("valor deve ser positivo, é %q", s)
	}
	return n, nil
}

// Duracao aceita a gramática do time.ParseDuration e recusa duração não
// positiva pelo mesmo motivo de InteiroPositivo: um intervalo de zero não é
// "sem intervalo", é um intervalo que nenhum agendador poderia honrar.
func Duracao(s string, padrao time.Duration) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return padrao, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("duração inválida %q", s)
	}
	if d <= 0 {
		return 0, fmt.Errorf("duração deve ser positiva, é %q", s)
	}
	return d, nil
}
