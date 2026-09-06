// Package logging é o logging estruturado do §95.
//
// Existe por dois motivos que o log.Printf não atende. O primeiro é o §95, que
// nomeia os campos: timestamp, level, component, dataset, event, message. O
// slog batiza "time" e "msg" por default, então sem tradução o log sairia fora
// da spec por descuido de biblioteca.
//
// O segundo é a proibição do mesmo §95: "não registrar payload completo por
// default. Não registrar senha." Uma proibição que vive só na cabeça de quem
// escreve a chamada é violada no primeiro debug às 3 da manhã, então ela vive
// aqui, no handler, onde vale para todo chamador — ver Secret e a lista de
// chaves sensíveis.
//
// O logger é seguro para uso concorrente: Component e Dataset DERIVAM um
// logger novo em vez de mutar o de origem, para que o server possa entregar
// um logger por dataset a partir de uma raiz compartilhada.
package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// Redacted é o que aparece no lugar de qualquer valor sensível. É texto e não
// string vazia de propósito: a linha de log tem de mostrar que houve um valor
// e que ele foi suprimido, senão o operador acha que o campo não foi
// preenchido e vai procurar o defeito no lugar errado.
const Redacted = "[REDACTADO]"

// Format é o formato de saída. JSON é o default de produção (o §95 pede log
// estruturado); texto existe para quem lê no terminal.
type Format int

// FormatJSON é o default de produção; FormatText existe para quem lê no
// terminal.
const (
	FormatJSON Format = iota
	FormatText
)

// Nomes de campo do §95.
const (
	fieldTimestamp = "timestamp"
	fieldComponent = "component"
	fieldDataset   = "dataset"
	fieldEvent     = "event"
	fieldMessage   = "message"
)

// sensitiveKeys são as chaves de atributo cujo VALOR nunca é registrado.
//
// A lista cobre as duas proibições do §95 nas grafias que aparecem no
// produto (o código é PT-BR, a config é camelCase em inglês). É comparação
// case-insensitive sobre a chave inteira, e não "contém": um `contains` sobre
// "token" apagaria `tokenizer_erros`, e um sobre "senha" apagaria
// `senha_invalida_total`, que são contadores que se quer ver.
//
// Note que usernameEnv/passwordEnv NÃO estão aqui: eles carregam o NOME da
// variável de ambiente, não o valor, e o nome é exatamente o que o operador
// precisa ver para diagnosticar credencial ausente.
var sensitiveKeys = map[string]bool{
	"password":      true,
	"passwd":        true,
	"senha":         true,
	"secret":        true,
	"segredo":       true,
	"token":         true,
	"apikey":        true,
	"payload":       true,
	"evento":        true,
	"event_payload": true,
	"body":          true,
	"record":        true,
	"value":         true,
}

// Secret marca um valor que nunca deve ser registrado, qualquer que seja a
// chave sob a qual ele apareça. É a defesa para o caso em que a chave é
// inocente e o valor não é — `bootstrap: user:senha@host`, por exemplo.
type Secret string

// LogValue implementa slog.LogValuer. É o próprio slog quem chama isto, antes
// de qualquer handler, então a substituição vale para JSON e texto sem código
// duplicado.
func (Secret) LogValue() slog.Value { return slog.StringValue(Redacted) }

// Logger emite as linhas do §95.
//
// Guarda a RAIZ (sem component/dataset) além do logger derivado, porque
// Component e Dataset precisam SUBSTITUIR e o slog.With só sabe acrescentar.
// Sem isso, um servidor que deriva Component("server") e entrega o logger a um
// consumer que deriva Component("kafka") emite o campo duas vezes — e chave
// repetida em JSON tem comportamento indefinido: jq, Loki e Elasticsearch
// escolhem uma delas por critérios diferentes, então o mesmo log lido por duas
// ferramentas discorda sobre quem emitiu a linha.
type Logger struct {
	root      *slog.Logger
	component string
	dataset   string
	extras    []any
	sl        *slog.Logger // raiz + component + dataset + extras
}

// New constrói o logger raiz. O chamador define o componente com Component.
func New(w io.Writer, f Format, level slog.Level) *Logger {
	opts := &slog.HandlerOptions{Level: level, ReplaceAttr: translateAndRedact}
	var h slog.Handler
	if f == FormatText {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	root := slog.New(h)
	return &Logger{root: root, sl: root}
}

// derive reconstrói o logger a partir da RAIZ, e não do derivado: é o que faz
// component e dataset substituírem em vez de acumularem.
func (l *Logger) derive(component, dataset string, extras []any) *Logger {
	sl := l.root
	if component != "" {
		sl = sl.With(slog.String(fieldComponent, component))
	}
	if dataset != "" {
		sl = sl.With(slog.String(fieldDataset, dataset))
	}
	if len(extras) > 0 {
		sl = sl.With(extras...)
	}
	return &Logger{root: l.root, component: component, dataset: dataset, extras: extras, sl: sl}
}

// translateAndRedact faz as duas coisas que o §95 exige, no único lugar por onde
// todo atributo passa: renomeia os campos embutidos do slog e suprime o valor
// das chaves sensíveis, em qualquer profundidade de grupo.
func translateAndRedact(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 {
		switch a.Key {
		case slog.TimeKey:
			a.Key = fieldTimestamp
			return a
		case slog.MessageKey:
			a.Key = fieldMessage
			return a
		}
	}
	if sensitiveKeys[strings.ToLower(a.Key)] {
		return slog.String(a.Key, Redacted)
	}
	return a
}

// Component deriva um logger para um subsistema (kafka, storage, query,
// server). Deriva, não muta: o logger de origem continua válido e limpo.
func (l *Logger) Component(name string) *Logger {
	return l.derive(name, l.dataset, l.extras)
}

// Dataset deriva um logger amarrado a um dataset. Sem esta chamada o campo
// simplesmente não aparece — um `dataset: ""` em toda linha de escopo global
// seria ruído que atrapalha o grep.
func (l *Logger) Dataset(name string) *Logger {
	return l.derive(l.component, name, l.extras)
}

// With deriva um logger com atributos fixos, úteis para amarrar partição e
// offset a toda linha de um laço.
func (l *Logger) With(args ...any) *Logger {
	newExtras := make([]any, 0, len(l.extras)+len(args))
	newExtras = append(newExtras, l.extras...)
	newExtras = append(newExtras, args...)
	return l.derive(l.component, l.dataset, newExtras)
}

// Enabled diz se o nível seria emitido, para o chamador poder pular a montagem
// de um atributo caro.
func (l *Logger) Enabled(level slog.Level) bool {
	return l.sl.Enabled(context.Background(), level)
}

// Debug emite no nível de depuração.
//
// Nos quatro níveis, `event` é o identificador estável que scripts e alertas
// casam (kafka.offset.commit, storage.recovery.divergencia) e `msg` é a prosa
// para humanos. Separar os dois é o que permite reescrever o texto sem
// quebrar alerta.
func (l *Logger) Debug(event, msg string, args ...any) { l.emit(slog.LevelDebug, event, msg, args) }

// Info emite no nível informativo.
func (l *Logger) Info(event, msg string, args ...any) { l.emit(slog.LevelInfo, event, msg, args) }

// Warn emite um aviso: algo saiu do esperado e o processo segue.
func (l *Logger) Warn(event, msg string, args ...any) { l.emit(slog.LevelWarn, event, msg, args) }

// Error emite uma falha: algo não foi feito.
func (l *Logger) Error(event, msg string, args ...any) { l.emit(slog.LevelError, event, msg, args) }

func (l *Logger) emit(level slog.Level, event, msg string, args []any) {
	if !l.sl.Enabled(context.Background(), level) {
		return
	}
	logArgs := make([]any, 0, len(args)+1)
	logArgs = append(logArgs, slog.String(fieldEvent, event))
	logArgs = append(logArgs, args...)
	l.sl.Log(context.Background(), level, msg, logArgs...)
}

// Discard é o logger que não escreve nada. Serve de default para quem ainda
// não recebeu um logger injetado, de modo que nenhum caminho precise checar
// nil antes de logar.
func Discard() *Logger { return New(io.Discard, FormatJSON, slog.LevelError+1) }
