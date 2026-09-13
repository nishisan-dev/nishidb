package server

import "context"

// principal é quem o middleware autenticou: o nome e o papel da tabela de
// usuários naquele request. Viaja no context do request porque os handlers de
// gestão de usuário precisam dos dois — o nome para decidir se é troca da
// PRÓPRIA senha e para a auditoria, o papel para a regra do handler e para a
// barreira de tabela virtual só-ADMIN.
type principal struct {
	username string
	role     string
}

type principalKey struct{}

func withPrincipal(ctx context.Context, p principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// principalFrom devolve o principal do request. ok=false significa que nenhum
// middleware autenticou este request — autenticação desligada ou o listener
// loopback, que é anônimo por construção.
func principalFrom(ctx context.Context) (principal, bool) {
	p, ok := ctx.Value(principalKey{}).(principal)
	return p, ok
}
