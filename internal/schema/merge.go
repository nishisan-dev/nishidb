package schema

// MergeSum é a política de escrita que ACUMULA: no merge, o valor do evento
// é somado ao que já estava no estado, em vez de substituí-lo.
//
// Existe para o campo que a origem emite como parcela — um contador de
// correlação, uma contagem de repetições —, cujo total é do NishiDB e não do
// produtor. Sem ela, o merge guarda a última parcela e o acumulado se perde.
//
// NÃO É IDEMPOTENTE, e isso é contrato: reprocessar o mesmo evento soma de
// novo. Um crash entre o commit do offset e a queda faz a janela reentregue
// somar duas vezes, e o valor fica ACIMA do real — aproximado, nunca exato.
// Onde a exatidão importa mais que o acumulado, a política certa continua
// sendo a sobrescrita, com a origem mandando o total.
const MergeSum = "sum"

// validateMergePolicies confere a chave `merge` de todos os campos.
//
// Roda numa passada própria, depois de key, sequence e mode estarem
// resolvidos: metade das regras é sobre o PAPEL do campo, e não sobre ele.
func validateMergePolicies(fields map[string]FieldSpec, keyField, seqField, mode string) error {
	for name, f := range fields {
		if f.Merge == "" {
			continue
		}
		if f.Merge != MergeSum {
			return specErrf("campo %q: merge %q inválido (conhecido: %q)", name, f.Merge, MergeSum)
		}
		if mode == "replace" {
			// No replace o estado anterior é descartado inteiro e o merge nem
			// roda: não há a que somar.
			return specErrf("campo %q: merge: %s não se aplica a mode: replace — "+
				"no replace o estado anterior é descartado inteiro", name, MergeSum)
		}
		if f.FrozenFrom != "" {
			// As duas são políticas de escrita do MESMO campo, e opostas: uma
			// preserva o primeiro valor, a outra acumula todos.
			return specErrf("campo %q: frozenFrom e merge: %s são políticas de escrita opostas", name, MergeSum)
		}
		if name == keyField {
			return specErrf("campo %q: merge não se aplica à key — ela identifica a entidade", name)
		}
		if seqField != "" && name == seqField {
			return specErrf("campo %q: merge não se aplica à sequence — "+
				"ela é a ordem do evento, não um valor da entidade", name)
		}
		if f.Type == "auto" {
			// Campo auto não tem tipo em plan-time: aceitar a política aqui
			// deixaria a soma valer ou não conforme o primeiro evento que
			// chegasse, e o operador não teria como saber qual foi.
			return specErrf("campo %q: merge: %s exige type: int64 declarado — "+
				"campo auto só tem tipo depois do primeiro evento", name, MergeSum)
		}
		if f.Type != "int64" {
			// A soma é de INTEIRO. Ponto flutuante acumula erro a cada
			// parcela, e um total que deriva do próprio histórico não pode
			// depender da ordem em que as parcelas chegaram.
			return specErrf("campo %q: merge: %s exige type: int64, e o tipo é %q", name, MergeSum, f.Type)
		}
	}
	return nil
}
