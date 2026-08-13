package checkpoint

// SetWriteTraceForTest instala o gravador da ordem de durabilidade do Write
// e devolve a função de restauração.
func SetWriteTraceForTest(fn func(step string)) func() {
	old := writeTrace
	writeTrace = fn
	return func() { writeTrace = old }
}
