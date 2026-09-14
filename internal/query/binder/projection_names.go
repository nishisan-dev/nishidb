package binder

import "strconv"

// projectionMapThreshold evita mapa na projeção pequena sem colisão e
// limita o custo quadrático da detecção a uma constante pequena.
const projectionMapThreshold = 32

// normalizeProjectionNames preserva todas as colunas e resolve colisões antes
// do ORDER BY. aliases contém os índices dos aliases explícitos, na ordem da
// projeção. O nome pedido é reservado mesmo se ocorrer depois da duplicata:
// x, x, x_1 vira x, x_2, x_1. Nada deste trabalho acontece por linha.
func normalizeProjectionNames(cols []BoundColumn, aliases []int) {
	if len(cols) <= projectionMapThreshold {
		duplicate := false
		for i, c := range cols {
			for j := 0; j < i; j++ {
				if cols[j].Name == c.Name {
					duplicate = true
					break
				}
			}
			if duplicate {
				break
			}
		}
		if !duplicate {
			return
		}
	}
	type owner struct {
		index    int
		explicit bool
		next     int
	}
	owners := make(map[string]owner, len(cols))
	for i, c := range cols {
		if _, exists := owners[c.Name]; !exists {
			owners[c.Name] = owner{index: i, next: 1}
		}
	}
	for _, i := range aliases {
		o := owners[cols[i].Name]
		if !o.explicit {
			o.index, o.explicit = i, true
			owners[cols[i].Name] = o
		}
	}
	for i := range cols {
		base := cols[i].Name
		o := owners[base]
		if o.index == i {
			continue
		}
		for {
			name := base + "_" + strconv.Itoa(o.next)
			o.next++
			if _, exists := owners[name]; exists {
				continue
			}
			cols[i].Name = name
			owners[name] = owner{index: i, next: 1}
			owners[base] = o
			break
		}
	}
}
