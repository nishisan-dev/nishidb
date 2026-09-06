// Package renametools é o código compartilhado entre scripts/renameinv e
// scripts/ifacerename: carregar o módulo com go/packages incluindo as
// variantes de teste, preservar a identidade de um pacote através dessas
// variantes e calcular os grupos de interface (componentes conexas do grafo
// (interface, método) ↔ (tipo concreto, método)).
package renametools
