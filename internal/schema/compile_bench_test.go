package schema

import "testing"

// Gate: FieldByName com []byte não pode alocar (elisão de map[string(b)]
// preservada mesmo com RLock).
func BenchmarkFieldByName(b *testing.B) {
	s, err := Parse([]byte(baseYAML), BuiltinDefaults)
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.Compile(newFakeDictB())
	if err != nil {
		b.Fatal(err)
	}
	name := []byte("Acct-Status-Type")
	alias := []byte("status")
	miss := []byte("inexistente")
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := c.FieldByName(name); !ok {
			b.Fatal("miss")
		}
		if _, ok := c.FieldByName(alias); !ok {
			b.Fatal("miss")
		}
		c.FieldByName(miss)
	}
}

// newFakeDictB evita depender de *testing.T do fake do arquivo de teste.
func newFakeDictB() *fakeDict { return newFakeDict() }
