package protocol

// Os corpos das rotas de gestão de usuário (issue #40): POST
// /v1/users/create, /v1/users/password, /v1/users/role e /v1/users/delete.
// A listagem não tem rota própria: é SQL sobre _meta.users, só para ADMIN.
//
// A senha viaja no corpo, nunca na URL nem em header próprio, pelo mesmo
// motivo de nunca ser flag no CLI: URL e header acabam em log de proxy. O
// transporte é o mesmo HTTPS do Basic que autentica o request. Nenhuma
// resposta ecoa a senha.

// UserCreateRequest é o corpo de POST /v1/users/create (ADMIN).
type UserCreateRequest struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	Password string `json:"password"`
}

// UserPasswordRequest é o corpo de POST /v1/users/password. Um ADMIN troca a
// senha de qualquer usuário; um READ só a própria, e a credencial corrente é
// o Basic do próprio request.
type UserPasswordRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// UserRoleRequest é o corpo de POST /v1/users/role (ADMIN).
type UserRoleRequest struct {
	Username string `json:"username"`
	Role     string `json:"role"`
}

// UserDeleteRequest é o corpo de POST /v1/users/delete (ADMIN).
type UserDeleteRequest struct {
	Username string `json:"username"`
}

// UserResponse é o corpo de 200 das quatro rotas: o usuário afetado e o papel
// que ele tem depois da operação. Role sai vazio só na remoção.
type UserResponse struct {
	Username string `json:"username"`
	Role     string `json:"role,omitempty"`
}
