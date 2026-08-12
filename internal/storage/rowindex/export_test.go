package rowindex

// SetApplyHookForTest injeta um callback executado após cada store do
// ApplyOverlay, para observar o estado no meio do apply.
func (l *Locations) SetApplyHookForTest(fn func()) { l.applyHook = fn }
