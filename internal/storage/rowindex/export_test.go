package rowindex

import "github.com/nishisan-dev/nishidb/internal/types"

// SetApplyHookForTest injeta um callback executado após cada store do
// ApplyOverlay, para observar o estado no meio do apply.
func (l *Locations) SetApplyHookForTest(fn func()) { l.applyHook = fn }

// SetStoreErrHookForTest injeta falhas nos stores do ApplyOverlay para
// exercitar o caminho de falha parcial.
func (l *Locations) SetStoreErrHookForTest(fn func(types.RowID) error) { l.storeErrHook = fn }
