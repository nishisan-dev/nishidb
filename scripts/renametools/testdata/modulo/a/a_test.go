package a

import "testing"

type escritorFalso struct{ n int }

func (f *escritorFalso) Apenda(chave []byte) error { f.n++; return nil }
func (f *escritorFalso) Fecha() error              { return nil }

func TestRodaConta(t *testing.T) {
	f := &escritorFalso{}
	c := Roda(f, [][]byte{[]byte("x"), []byte("y")})
	if c.Total != 2 || f.n != 2 {
		t.Fatalf("Total=%d n=%d", c.Total, f.n)
	}
	t.Run("vazio conta zero", func(t *testing.T) {
		if Roda(f, nil).Total != 0 {
			t.Fatal("contou")
		}
	})
}
