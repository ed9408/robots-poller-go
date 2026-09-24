// Package handler coordina la consulta, seleccion, ejecucion y persistencia
// de los robots asociados a un cliente.
package handler

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"orchestrator/internal/models"
)

// Repository es la dependencia de persistencia que necesita el orquestador.
// La interfaz permite probar el flujo sin conectar con DynamoDB.
type Repository interface {
	ListAllRobots(ctx context.Context) ([]models.RobotState, error)
	TryAcquireLock(ctx context.Context, robotId string) (bool, error)
	ReleaseLock(ctx context.Context, robotId string) error
	RecordExecution(ctx context.Context, robotId string, result models.ExecutionResult) error
}

// RobotCaller es la dependencia que invoca el endpoint HTTP de cada robot.
type RobotCaller interface {
	Call(ctx context.Context, url string) (httpCode uint16, lastId *string, err error)
}

// TierFilter decide si un robot pertenece al proceso que está corriendo
// este Handler (Lambda vs EC2), según su interval_seconds.
type TierFilter func(state models.RobotState) bool

// FastTier: robots con intervalo menor a 60s — los procesa EC2.
func FastTier(state models.RobotState) bool {
	return state.IntervalSecs < 60
}

// StandardTier: robots con intervalo de 60s o más — los procesa Lambda.
func StandardTier(state models.RobotState) bool {
	return state.IntervalSecs >= 60
}

// Handler coordina una ejecucion de cliente usando un repositorio y un caller
// HTTP, con un limite de concurrencia por invocacion.
type Handler struct {
	repo           Repository
	caller         RobotCaller
	maxConcurrency uint8
	tierFilter     TierFilter
}

// New crea un handler. Un limite de concurrencia menor que uno se normaliza a
// uno para garantizar que siempre pueda procesarse un robot.
func New(repo Repository, caller RobotCaller, maxConcurrency uint8, tierFilter TierFilter) *Handler {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}

	if tierFilter == nil {
		tierFilter = func(models.RobotState) bool { return true }
	}

	return &Handler{repo: repo, caller: caller, maxConcurrency: maxConcurrency, tierFilter: tierFilter}
}

// Handle procesa todos los robots de un cliente que estén "debidos" según
// su propio intervalo individual, ejecutándolos en paralelo con un límite
// de concurrencia para no saturar el CRM/ERP del cliente.
func (h *Handler) Handle(ctx context.Context) (models.PollExecutionSummary, error) {
	summary := models.PollExecutionSummary{}
	states, err := h.repo.ListAllRobots(ctx)

	if err != nil {
		return summary, fmt.Errorf("listar robots: %w", err)
	}

	now := time.Now().UTC()
	due := make([]models.RobotState, 0, len(states))

	for _, s := range states {
		if h.tierFilter(s) && s.IsDue(now) {
			due = append(due, s)
		}
	}

	summary.TotalDue = len(due)

	if len(due) == 0 {
		return summary, nil
	}

	results := h.executeAll(ctx, due)
	summary.Results = results

	for _, res := range results {
		switch res.Status {
		case "SUCCESS":
			summary.Succeeded++
		case "SKIPPED_LOCKED":
			summary.Skipped++
		default:
			summary.Failed++
		}
	}

	if summary.Failed > 0 {
		return summary, fmt.Errorf("%d de %d robots fallaron", summary.Failed, summary.TotalDue)
	}

	return summary, nil
}

// executeAll ejecuta los robots debidos en paralelo, limitando la concurrencia
// con un semaforo implementado mediante un canal con buffer.
func (h *Handler) executeAll(ctx context.Context, due []models.RobotState) []models.ExecutionResult {
	results := make([]models.ExecutionResult, len(due))
	sem := make(chan struct{}, h.maxConcurrency)
	var wg sync.WaitGroup

	for i, state := range due {
		wg.Add(1)
		go func(idx int, s models.RobotState) {
			defer wg.Done()
			sem <- struct{}{}        // adquiere slot
			defer func() { <-sem }() // libera slot

			results[idx] = h.executeOne(ctx, s)
		}(i, state)
	}

	wg.Wait()
	return results
}

// executeOne maneja el lock, la llamada HTTP y el registro de un unico robot.
// Los errores de este robot se convierten en resultado y no detienen a los
// demas robots.
func (h *Handler) executeOne(ctx context.Context, state models.RobotState) models.ExecutionResult {
	locked, err := h.repo.TryAcquireLock(ctx, state.RobotID)

	if err != nil {
		return models.ExecutionResult{
			RobotID:  state.RobotID,
			Status:   "FAILURE",
			ErrorMsg: fmt.Sprintf("adquirir lock: %v", err),
		}
	}

	if !locked {
		return models.ExecutionResult{
			RobotID: state.RobotID,
			Status:  "SKIPPED_LOCKED",
		}
	}

	defer h.repo.ReleaseLock(ctx, state.RobotID)

	endpointUrl, err := addLastId(state.EndpointURL, state.LastId)

	if err != nil {
		return models.ExecutionResult{
			RobotID:  state.RobotID,
			Status:   "FAILURE",
			ErrorMsg: fmt.Sprintf("construir URL del robot %s: %v", state.RobotID, err),
		}
	}

	start := time.Now()
	httpCode, lastId, callErr := h.caller.Call(ctx, endpointUrl)
	duration := time.Since(start).Milliseconds()

	result := models.ExecutionResult{
		RobotID:    state.RobotID,
		HttpCode:   httpCode,
		DurationMs: uint64(duration),
		LastId:     lastId,
	}

	if callErr != nil {
		result.Status = "FAILURE"
		result.ErrorMsg = callErr.Error()
	} else {
		result.Status = "SUCCESS"
	}

	if recErr := h.repo.RecordExecution(ctx, state.RobotID, result); recErr != nil {
		// el robot sí se ejecutó, pero no pudimos registrar el resultado;
		// lo marcamos como fallo para que quede visible en el summary,
		// aunque el HttpCode/DurationMs reales ya están en el result.
		result.Status = "FAILURE"
		result.ErrorMsg = fmt.Sprintf("ejecutado pero falló registro: %v", recErr)
	}

	return result
}

func addLastId(endpoint string, lastId *string) (string, error) {
	if lastId == nil || *lastId == "" {
		return endpoint, nil
	}

	parsedUrl, err := url.Parse(endpoint)

	if err != nil {
		return "", fmt.Errorf("parse endpoint URL: %w", err)
	}

	query := parsedUrl.Query()
	query.Set("id", *lastId)
	parsedUrl.RawQuery = query.Encode()

	return parsedUrl.String(), nil
}
