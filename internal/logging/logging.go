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
// aqui, no handler, onde vale para todo chamador — ver Segredo e a lista de
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

// Redactado é o que aparece no lugar de qualquer valor sensível. É texto e não
// string vazia de propósito: a linha de log tem de mostrar que houve um valor
// e que ele foi suprimido, senão o operador acha que o campo não foi
// preenchido e vai procurar o defeito no lugar errado.
const Redactado = "[REDACTADO]"

// Formato de saída. JSON é o default de produção (o §95 pede log estruturado);
// texto existe para quem lê no terminal.
type Formato int

// FormatoJSON é o default de produção; FormatoTexto existe para quem lê no
// terminal.
const (
	FormatoJSON Formato = iota
	FormatoTexto
)

// Nomes de campo do §95.
const (
	campoTimestamp = "timestamp"
	campoComponent = "component"
	campoDataset   = "dataset"
	campoEvent     = "event"
	campoMessage   = "message"
)

// chavesSensiveis são as chaves de atributo cujo VALOR nunca é registrado.
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
var chavesSensiveis = map[string]bool{
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

// Segredo marca um valor que nunca deve ser registrado, qualquer que seja a
// chave sob a qual ele apareça. É a defesa para o caso em que a chave é
// inocente e o valor não é — `bootstrap: user:senha@host`, por exemplo.
type Segredo string

// LogValue implementa slog.LogValuer. É o próprio slog quem chama isto, antes
// de qualquer handler, então a substituição vale para JSON e texto sem código
// duplicado.
func (Segredo) LogValue() slog.Value { return slog.StringValue(Redactado) }

// Logger emite as linhas do §95.
type Logger struct {
	sl *slog.Logger
}

// New constrói o logger raiz. O chamador define o componente com Component.
func New(w io.Writer, f Formato, nivel slog.Level) *Logger {
	opts := &slog.HandlerOptions{Level: nivel, ReplaceAttr: traduzERedige}
	var h slog.Handler
	if f == FormatoTexto {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return &Logger{sl: slog.New(h)}
}

// traduzERedige faz as duas coisas que o §95 exige, no único lugar por onde
// todo atributo passa: renomeia os campos embutidos do slog e suprime o valor
// das chaves sensíveis, em qualquer profundidade de grupo.
func traduzERedige(grupos []string, a slog.Attr) slog.Attr {
	if len(grupos) == 0 {
		switch a.Key {
		case slog.TimeKey:
			a.Key = campoTimestamp
			return a
		case slog.MessageKey:
			a.Key = campoMessage
			return a
		}
	}
	if chavesSensiveis[strings.ToLower(a.Key)] {
		return slog.String(a.Key, Redactado)
	}
	return a
}

// Component deriva um logger para um subsistema (kafka, storage, query,
// server). Deriva, não muta: o logger de origem continua válido e limpo.
func (l *Logger) Component(nome string) *Logger {
	return &Logger{sl: l.sl.With(slog.String(campoComponent, nome))}
}

// Dataset deriva um logger amarrado a um dataset. Sem esta chamada o campo
// simplesmente não aparece — um `dataset: ""` em toda linha de escopo global
// seria ruído que atrapalha o grep.
func (l *Logger) Dataset(nome string) *Logger {
	return &Logger{sl: l.sl.With(slog.String(campoDataset, nome))}
}

// With deriva um logger com atributos fixos, úteis para amarrar partição e
// offset a toda linha de um laço.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{sl: l.sl.With(args...)}
}

// Enabled diz se o nível seria emitido, para o chamador poder pular a montagem
// de um atributo caro.
func (l *Logger) Enabled(nivel slog.Level) bool {
	return l.sl.Enabled(context.Background(), nivel)
}

// Debug emite no nível de depuração.
//
// Nos quatro níveis, `event` é o identificador estável que scripts e alertas
// casam (kafka.offset.commit, storage.recovery.divergencia) e `msg` é a prosa
// para humanos. Separar os dois é o que permite reescrever o texto sem
// quebrar alerta.
func (l *Logger) Debug(event, msg string, args ...any) { l.emite(slog.LevelDebug, event, msg, args) }

// Info emite no nível informativo.
func (l *Logger) Info(event, msg string, args ...any) { l.emite(slog.LevelInfo, event, msg, args) }

// Warn emite um aviso: algo saiu do esperado e o processo segue.
func (l *Logger) Warn(event, msg string, args ...any) { l.emite(slog.LevelWarn, event, msg, args) }

// Error emite uma falha: algo não foi feito.
func (l *Logger) Error(event, msg string, args ...any) { l.emite(slog.LevelError, event, msg, args) }

func (l *Logger) emite(nivel slog.Level, event, msg string, args []any) {
	if !l.sl.Enabled(context.Background(), nivel) {
		return
	}
	todos := make([]any, 0, len(args)+1)
	todos = append(todos, slog.String(campoEvent, event))
	todos = append(todos, args...)
	l.sl.Log(context.Background(), nivel, msg, todos...)
}

// Descartado é o logger que não escreve nada. Serve de default para quem ainda
// não recebeu um logger injetado, de modo que nenhum caminho precise checar
// nil antes de logar.
func Descartado() *Logger { return New(io.Discard, FormatoJSON, slog.LevelError+1) }
