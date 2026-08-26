"""Lê o SQL do lado do CLIENTE para responder duas perguntas — e nenhuma outra:
qual dataset o ``FROM`` nomeia, e quantos ``?`` a query tem.

**O dataset precisa sair daqui** porque o corpo de ``POST /v1/query`` traz o
nome do dataset num campo separado do SQL, e o servidor RECUSA a consulta
quando os dois divergem. Sem este extrator, uma conexão só conseguiria
consultar o dataset que a string de conexão nomeia — todos os outros morreriam
no bind.

**O palpite errado nunca devolve dado de outra tabela.** É a garantia que torna
este atalho aceitável: quem confere é o servidor, e a divergência vira erro,
nunca resultado. Por isso este scanner é um EXTRATOR DE PALPITE e jamais um
validador — ele não recusa SQL nenhum, **não levanta exceção para entrada
nenhuma** e devolve ``None`` quando não sabe. Recusar aqui trocaria a mensagem
do servidor, que tem posição de byte e nome de coluna, por uma do cliente, que
não tem nem uma coisa nem outra.

**Não é um parser, e não deve virar um.** A gramática de verdade mora em
``internal/query/parser``, e uma segunda gramática aqui divergiria da primeira
no dia em que a v2 acrescentasse qualquer coisa. Porte fiel de
``SqlScanner.java``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Varredura:
    """O resultado da única passada sobre o texto."""

    #: O dataset nomeado no FROM, com a caixa EXATA em que foi escrito, ou None
    #: quando a query não o nomeia de forma reconhecível.
    #:
    #: A caixa é preservada de propósito: identificadores do NishiDB são
    #: case-sensitive e o binder resolve nome exato. Normalizar aqui faria o
    #: client pedir um dataset que não existe.
    dataset: str | None
    #: Quantos `?` há fora de literal e de identificador entre aspas.
    parametros: int


def _espaco(c: str) -> bool:
    """Os brancos do lexer do servidor.

    Lista fechada em vez de ``str.isspace()``: o ``isspace`` do Python é
    Unicode (casa NBSP, EM SPACE e mais uma dúzia) e o lexer do servidor é
    ASCII puro. Duas regras de branco fariam o client enxergar uma fronteira
    de token que o servidor não enxerga.
    """
    return c in " \t\n\r\v\f"


def _inicio_de_ident(c: str) -> bool:
    return c == "_" or ("a" <= c <= "z") or ("A" <= c <= "Z")


def _parte_de_ident(c: str) -> bool:
    return _inicio_de_ident(c) or ("0" <= c <= "9")


def _igual_sem_caixa_ascii(a: str, b: str) -> bool:
    """Comparação ASCII, e não ``a.lower() == b.lower()``.

    O ``lower()``/``casefold()`` do Python é Unicode, e o do lexer do servidor
    é ASCII puro — o godoc de ``keywordType`` registra o caso do sinal de
    Kelvin (U+212A), que dobra para 'k'. Com o folding Unicode, ``SELECT x
    FROM t`` escrito com esse caractere seria reconhecido aqui e não lá.
    """
    if len(a) != len(b):
        return False
    return all(_minuscula_ascii(x) == _minuscula_ascii(y) for x, y in zip(a, b, strict=True))


def _minuscula_ascii(c: str) -> str:
    return chr(ord(c) + 32) if "A" <= c <= "Z" else c


def _fim_do_texto_entre(sql: str, ini: int, aspa: str) -> int:
    """O índice logo APÓS a aspa de fechamento do texto que começa em ``ini``,
    ou -1 quando não há fechamento.

    A regra de escape é a do lexer do servidor: a aspa duplicada é um escape,
    não um fechamento seguido de abertura. Sem isso, ``'a''?''b'`` teria o
    ``?`` lido como estando FORA do literal, e ele viraria um parâmetro que o
    servidor não tem.
    """
    n = len(sql)
    i = ini + 1
    while i < n:
        if sql[i] != aspa:
            i += 1
            continue
        if i + 1 < n and sql[i + 1] == aspa:
            i += 2
            continue
        return i + 1
    return -1


def _desescapa(corpo: str, aspa: str) -> str:
    """Desfaz a aspa duplicada, a única forma de escape que o lexer conhece."""
    return corpo.replace(aspa + aspa, aspa)


def _seguido_de_ponto(sql: str, de: int, n: int) -> bool:
    """Se o próximo caractere não-branco a partir de ``de`` é um ponto."""
    j = de
    while j < n and _espaco(sql[j]):
        j += 1
    return j < n and sql[j] == "."


def varre(sql: str) -> Varredura:
    """Varre o SQL uma vez e responde as duas perguntas. Nunca levanta."""
    dataset: str | None = None
    parametros = 0
    # Ligado quando a palavra FROM acaba de ser lida: o PRÓXIMO token tem de
    # ser o nome. Se for outra coisa, o palpite é abandonado — e não adiado
    # para um FROM posterior, porque a v1 não tem subquery nem JOIN e um
    # segundo FROM é SQL inválido, não outro escopo.
    esperando_nome = False
    # Separa "ainda não achei o FROM" de "achei e decidi não palpitar". Sem
    # ele, o dataset nulo do nome qualificado faria um FROM posterior no texto
    # voltar a capturar um nome.
    visto_from = False

    i = 0
    n = len(sql)
    while i < n:
        c = sql[i]

        if _espaco(c):
            i += 1
            continue

        if c == "'":
            fim = _fim_do_texto_entre(sql, i, "'")
            if fim < 0:
                # Literal sem fechamento: o resto da entrada está DENTRO dele.
                # Continuar a varrer contaria como parâmetro um '?' que é
                # texto, e o servidor vai recusar a query de qualquer forma.
                break
            i = fim
            esperando_nome = False
            continue

        if c == '"':
            fim = _fim_do_texto_entre(sql, i, '"')
            if fim < 0:
                break
            if esperando_nome:
                dataset = _desescapa(sql[i + 1 : fim - 1], '"')
                esperando_nome = False
                # NÃO marca visto_from aqui, e a assimetria com o ramo do
                # identificador bare é do SqlScanner.java, copiada de
                # propósito. Ela só é observável em SQL com dois FROM, que é
                # inválido e o servidor recusa de qualquer forma — e um client
                # que "consertasse" isso passaria a discordar do outro sobre
                # qual dataset a mesma query nomeia.
            i = fim
            continue

        if c == "?":
            parametros += 1
            i += 1
            esperando_nome = False
            continue

        if _inicio_de_ident(c):
            fim = i + 1
            while fim < n and _parte_de_ident(sql[fim]):
                fim += 1
            palavra = sql[i:fim]
            if esperando_nome:
                # O dataset NÃO encerra a varredura: os '?' vêm depois dele, no
                # WHERE. Parar aqui zeraria a contagem de parâmetros de toda
                # query com parâmetro.
                visto_from = True
                esperando_nome = False
                # Nome QUALIFICADO (`_meta.fields`) abandona o palpite: o ponto
                # não é parte de identificador aqui, então capturar `palavra`
                # entregaria "_meta" — um dataset que não existe. Devolver None
                # faz o client usar o dataset da conexão, que é o certo.
                if not _seguido_de_ponto(sql, fim, n):
                    dataset = palavra
                i = fim
                continue
            if not visto_from and _igual_sem_caixa_ascii(palavra, "FROM"):
                esperando_nome = True
            i = fim
            continue

        # Operador, pontuação, número, byte estranho: nada disso muda as duas
        # respostas. Um caractere de cada vez, porque o único risco seria pular
        # por cima de uma aspa.
        i += 1
        esperando_nome = False

    return Varredura(dataset=dataset, parametros=parametros)
