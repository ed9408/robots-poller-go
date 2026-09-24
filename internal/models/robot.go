// Package models define los eventos, estados y resultados que atraviesan el
// flujo de orquestacion.
package models

import "time"

// PollEvent es el payload que EventBridge Scheduler envía a la Lambda.
// Ya no necesita ningún dato: la Lambda decide por sí misma qué robots
// de qué clientes le tocan en cada ciclo, consultando DynamoDB.
type PollEvent struct{}

// ClientEvent es el payload que EventBridge envia a la Lambda.
/* type ClientEvent struct {
	ClientID string `json:"client_id"`
} */

// RobotState representa el ítem SK=STATE en DynamoDB.
// Se agregan ClientID y RobotID como atributos explícitos porque ahora
// se necesitan para el filtrado y para el GSI por cliente.
type RobotState struct {
	ClientID        string    `dynamodbav:"client_id"`
	RobotID         string    `dynamodbav:"robot_id"`
	EndpointURL     string    `dynamodbav:"endpoint_url"`
	IntervalSecs    int       `dynamodbav:"interval_seconds"`
	Enabled         bool      `dynamodbav:"enabled"`
	LockStatus      string    `dynamodbav:"lock_status"`
	LastExecutionAt time.Time `dynamodbav:"last_execution_at,unixtime"`
	LastId          *string   `dynamodbav:"ult_id"`
}

// IsDue determina si, dado el momento actual, este robot ya debe ejecutarse
// según su propio intervalo individual.
func (s *RobotState) IsDue(now time.Time) bool {
	if !s.Enabled {
		return false
	}

	if s.LastExecutionAt.IsZero() {
		return true
	}

	return now.Sub(s.LastExecutionAt) >= time.Duration(s.IntervalSecs)*time.Second
}

// ExecutionResult es el resultado de invocar el endpoint de un robot.
type ExecutionResult struct {
	RobotID    string
	Status     string // "SUCCESS" | "FAILURE" | "SKIPPED_LOCKED"
	HttpCode   uint16
	DurationMs uint64
	ErrorMsg   string
	LastId     *string
}

// ClientExecutionSummary agrupa el resultado de procesar todos los robots
// debidos de un cliente en un solo ciclo del poller.
/* type ClientExecutionSummary struct {
	ClientID  string
	TotalDue  int
	Succeeded int
	Failed    int
	Skipped   int
	Results   []ExecutionResult
} */

// PollExecutionSummary agrupa el resultado de un ciclo completo del
// poller, ahora a nivel global (todos los clientes), no por cliente.
type PollExecutionSummary struct {
	TotalDue  int
	Succeeded int
	Failed    int
	Skipped   int
	Results   []ExecutionResult
}
