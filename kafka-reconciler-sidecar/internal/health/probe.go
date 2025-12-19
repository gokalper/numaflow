package health

import (
	"encoding/json"
	"net/http"
	"time"
)

// Probe provides HTTP health check endpoint
type Probe struct {
	startTime time.Time
}

// NewProbe creates a health probe
func NewProbe() *Probe {
	return &Probe{
		startTime: time.Now(),
	}
}

// Handler returns HTTP handler for /healthz
func (p *Probe) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uptime := time.Since(p.startTime).Seconds()

		response := map[string]interface{}{
			"healthy":        true,
			"version":        "v1.0.0",
			"uptime_seconds": int64(uptime),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}
}
