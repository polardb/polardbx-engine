package transfer

import "sync/atomic"

type Metrics struct {
	data map[string]*uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		data: make(map[string]*uint64),
	}
}

func (m *Metrics) Register(key string) {
	m.data[key] = new(uint64)
}

func (m *Metrics) Record(key string) {
	atomic.AddUint64(m.data[key], 1)
}

func (m *Metrics) CopyAndReset() map[string]uint64 {
	old := make(map[string]uint64)
	for k, counter := range m.data {
		old[k] = atomic.SwapUint64(counter, 0)
	}
	return old
}
