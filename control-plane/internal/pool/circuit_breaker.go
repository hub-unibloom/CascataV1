package pool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CircuitState representa o estado atual do breaker
type CircuitState string

const (
	StateClosed   CircuitState = "CLOSED"
	StateOpen     CircuitState = "OPEN"
	StateHalfOpen CircuitState = "HALF_OPEN"
)

// CircuitBreaker guarda o estado do pool para uma instância específica (Req-3.5.11)
type CircuitBreaker struct {
	mu           sync.RWMutex
	instanceID   string
	state        CircuitState
	failureCount int
	lastCheck    time.Time
}

// NewCircuitBreaker cria o limitador e protetor de falhas do YugabyteDB
func NewCircuitBreaker(instanceHost string) *CircuitBreaker {
	return &CircuitBreaker{
		instanceID: instanceHost,
		state:      StateClosed,
	}
}

// RecordFailure incrementa as falhas e abre o circuito se ultrapassar o limiar.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	// Heurística de exemplo base: 10 falhas consecutivas apontam que o YugabyteDB (YSQL CM) está caído.
	if cb.failureCount > 10 && cb.state == StateClosed {
		cb.state = StateOpen
		cb.lastCheck = time.Now()
		// dispararia aqui notificação ao Pingora via Unix Socket
		notifyPingora(cb.instanceID, false)
	}
}

// Check Probe verifica se a transição para HALF_OPEN e recovery teve sucesso
func (cb *CircuitBreaker) ProbeRecovery(ctx context.Context, orch *Orchestrator) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateOpen && time.Since(cb.lastCheck) > 30*time.Second {
		cb.state = StateHalfOpen
		// Faz ping real na base
		if err := orch.PingHealth(ctx); err == nil {
			cb.state = StateClosed
			cb.failureCount = 0
			notifyPingora(cb.instanceID, true)
		} else {
			// Voltou a falhar
			cb.state = StateOpen
			cb.lastCheck = time.Now()
		}
	}
}

// Mock da pipe unix para o Gateway (Rust/Pingora)
func notifyPingora(instance string, isHealthy bool) {
	if !isHealthy {
		fmt.Printf("[CRITICAL] Notifying Pingora: Circuit to %s is OPEN\n", instance)
	} else {
		fmt.Printf("[INFO] Notifying Pingora: Circuit to %s recovered (CLOSED)\n", instance)
	}
}
