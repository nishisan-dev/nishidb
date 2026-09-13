// As rotas de gestão de usuário (issue #40): POST /v1/users/create,
// /v1/users/password, /v1/users/role e /v1/users/delete.
//
// Toda gestão exige um principal AUTENTICADO. Com a autenticação desligada o
// withAuth serve o mux cru, e no plano loopback não há middleware nenhum: nos
// dois casos o request chega sem principal e é recusado com FORBIDDEN — não
// existe gestão anônima. As três rotas ADMIN já são barradas antes, pelo
// papel registrado (e pelo loopback, que recusa ADMIN); a de senha é READ
// porque um usuário READ troca a PRÓPRIA senha, e a regra "só a própria" mora
// no handler, onde o nome do principal é conhecido.
//
// A listagem não tem rota: é SQL sobre _meta.users, só para ADMIN.

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/users"
)

// handleUserCreate serve POST /v1/users/create (ADMIN).
func (s *Server) handleUserCreate(w http.ResponseWriter, r *http.Request) {
	p, ok := s.userManager(w, r)
	if !ok {
		return
	}
	var req protocol.UserCreateRequest
	if !s.decodeUserRequest(w, r, &req) {
		return
	}
	if err := s.users.Create(actorOf(p, r), req.Username, req.Role, []byte(req.Password)); err != nil {
		s.rejectUserError(w, r, err)
		return
	}
	s.respondeJSON(w, protocol.UserResponse{Username: req.Username, Role: req.Role})
}

// handleUserPassword serve POST /v1/users/password (READ). Um ADMIN troca a
// senha de qualquer usuário; os demais, só a própria.
func (s *Server) handleUserPassword(w http.ResponseWriter, r *http.Request) {
	p, ok := s.userManager(w, r)
	if !ok {
		return
	}
	var req protocol.UserPasswordRequest
	if !s.decodeUserRequest(w, r, &req) {
		return
	}
	// A recusa vem ANTES de consultar a tabela: um READ que pergunta pela
	// senha de outro nome não pode descobrir, pela diferença entre FORBIDDEN e
	// UNKNOWN_USER, se aquele nome existe.
	if p.role != config.RoleAdmin && req.Username != p.username {
		s.reject(w, r, http.StatusForbidden, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeForbidden,
			Message: "a READ user can only change its own password",
		})
		return
	}
	if err := s.users.SetPassword(actorOf(p, r), req.Username, []byte(req.Password)); err != nil {
		s.rejectUserError(w, r, err)
		return
	}
	s.respondeJSON(w, protocol.UserResponse{Username: req.Username, Role: s.roleOf(req.Username)})
}

// handleUserRole serve POST /v1/users/role (ADMIN).
func (s *Server) handleUserRole(w http.ResponseWriter, r *http.Request) {
	p, ok := s.userManager(w, r)
	if !ok {
		return
	}
	var req protocol.UserRoleRequest
	if !s.decodeUserRequest(w, r, &req) {
		return
	}
	if err := s.users.SetRole(actorOf(p, r), req.Username, req.Role); err != nil {
		s.rejectUserError(w, r, err)
		return
	}
	s.respondeJSON(w, protocol.UserResponse{Username: req.Username, Role: req.Role})
}

// handleUserDelete serve POST /v1/users/delete (ADMIN).
func (s *Server) handleUserDelete(w http.ResponseWriter, r *http.Request) {
	p, ok := s.userManager(w, r)
	if !ok {
		return
	}
	var req protocol.UserDeleteRequest
	if !s.decodeUserRequest(w, r, &req) {
		return
	}
	if err := s.users.Delete(actorOf(p, r), req.Username); err != nil {
		s.rejectUserError(w, r, err)
		return
	}
	s.respondeJSON(w, protocol.UserResponse{Username: req.Username})
}

// userManager aplica o que as quatro rotas têm em comum antes de ler o corpo:
// método POST e principal autenticado.
func (s *Server) userManager(w http.ResponseWriter, r *http.Request) (principal, bool) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.reject(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeMethod,
			Message: r.Method + " is not accepted on this route; use POST",
		})
		return principal{}, false
	}
	p, ok := principalFrom(r.Context())
	if !ok || s.users == nil {
		s.reject(w, r, http.StatusForbidden, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeForbidden,
			Message: "user management requires an authenticated request on the authenticated listener",
		})
		return principal{}, false
	}
	return p, true
}

// decodeUserRequest lê o corpo JSON obrigatório, com o mesmo teto e a mesma
// recusa de campo desconhecido das outras rotas mutantes. A mensagem de corpo
// inválido NÃO repete o erro do decoder: ele pode citar o trecho do JSON, e o
// trecho pode ser a senha.
func (s *Server) decodeUserRequest(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, protocol.MaxRequestBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil {
		return true
	}
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		s.reject(w, r, http.StatusRequestEntityTooLarge, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeBodyTooLarge,
			Message: fmt.Sprintf("request body above the %d byte limit", protocol.MaxRequestBody),
		})
	case errors.Is(err, io.EOF):
		s.reject(w, r, http.StatusBadRequest, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeBadRequest,
			Message: "request body is required",
		})
	default:
		s.reject(w, r, http.StatusBadRequest, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeBadRequest,
			Message: "request body is not a valid JSON object for this route",
		})
	}
	return false
}

// rejectUserError traduz o erro da tabela em status e código.
func (s *Server) rejectUserError(w http.ResponseWriter, r *http.Request, err error) {
	status, fail := userError(err)
	s.reject(w, r, status, datasetDesconhecido, fail)
}

func userError(err error) (int, protocol.ErrorDetail) {
	switch {
	case errors.Is(err, users.ErrUserExists):
		return http.StatusConflict, protocol.ErrorDetail{Code: protocol.CodeUserExists, Message: err.Error()}
	case errors.Is(err, users.ErrUnknownUser):
		return http.StatusNotFound, protocol.ErrorDetail{Code: protocol.CodeUnknownUser, Message: err.Error()}
	case errors.Is(err, users.ErrLastAdmin):
		return http.StatusConflict, protocol.ErrorDetail{Code: protocol.CodeLastAdmin, Message: err.Error()}
	case errors.Is(err, users.ErrWeakPassword):
		return http.StatusBadRequest, protocol.ErrorDetail{Code: protocol.CodeWeakPassword, Message: err.Error()}
	case errors.Is(err, users.ErrInvalidUsername), errors.Is(err, users.ErrInvalidRole):
		return http.StatusBadRequest, protocol.ErrorDetail{Code: protocol.CodeBadParameter, Message: err.Error()}
	case errors.Is(err, users.ErrWriterFailed):
		return http.StatusInternalServerError, protocol.ErrorDetail{Code: protocol.CodeWriterFailed, Message: err.Error()}
	default:
		return http.StatusInternalServerError, protocol.ErrorDetail{Code: protocol.CodeInternal, Message: err.Error()}
	}
}

func actorOf(p principal, r *http.Request) users.Actor {
	return users.Actor{Name: p.username, Remote: r.RemoteAddr}
}

// roleOf devolve o papel corrente de um usuário, para a resposta da troca de
// senha. Vazio se ele sumiu entre a troca e a leitura.
func (s *Server) roleOf(username string) string {
	for _, u := range s.users.Users() {
		if u.Username == username {
			return u.Role
		}
	}
	return ""
}
