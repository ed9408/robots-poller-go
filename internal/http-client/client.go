// Package httpclient encapsula las llamadas HTTP a los endpoints de robots.
package httpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type robotResponse struct {
	UltId *string `json:"ult_id"`
}

// RobotCaller ejecuta solicitudes HTTP con un timeout compartido para todos
// los robots procesados por una invocacion.
type RobotCaller struct {
	client *http.Client
}

// NewRobotCaller crea un caller con el timeout indicado.
func NewRobotCaller(timeout time.Duration) *RobotCaller {
	return &RobotCaller{
		client: &http.Client{Timeout: timeout},
	}
}

// Call ejecuta el endpoint del robot y retorna el codigo HTTP recibido junto
// con el ult_id opcional de una respuesta JSON exitosa. Las respuestas HTTP
// 400 o superiores se devuelven como error.
func (c *RobotCaller) Call(ctx context.Context, url string) (uint16, *string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.client.Do(req)

	if err != nil {
		return 0, nil, fmt.Errorf("http call: %w", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return uint16(resp.StatusCode), nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return uint16(resp.StatusCode), nil, fmt.Errorf("respuesta HTTP %d del endpoint", resp.StatusCode)
	}

	if len(body) == 0 {
		return uint16(resp.StatusCode), nil, nil
	}

	var response robotResponse

	if err := json.Unmarshal(body, &response); err != nil {
		return uint16(resp.StatusCode), nil, fmt.Errorf("decode response body: %w", err)
	}

	return uint16(resp.StatusCode), response.UltId, nil
}
