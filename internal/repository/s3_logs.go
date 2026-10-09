package repository

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"orchestrator/internal/models"
)

// ErrorLogger guarda el log de una ejecucion fallida y retorna la key S3 donde
// quedara (o ya quedo) el objeto. La implementan ErrorLogStore (sube cada
// error de inmediato, para Lambda) y BatchingErrorLogStore (agrupa en lotes,
// para EC2).
type ErrorLogger interface {
	SaveError(ctx context.Context, robotId string, now time.Time, result models.ExecutionResult) (string, error)
}

// ErrorLogStore guarda en S3 los logs de las ejecuciones que fallaron.
// Las ejecuciones correctas no se registran. La retencion y el cambio de
// clase de almacenamiento se configuran con reglas de Lifecycle en el bucket.
type ErrorLogStore struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewErrorLogStore crea un almacen de logs de error para el bucket y prefijo
// indicados. El prefijo se normaliza sin "/" al inicio ni al final.
func NewErrorLogStore(client *s3.Client, bucket, prefix string) *ErrorLogStore {
	return &ErrorLogStore{
		client: client,
		bucket: bucket,
		prefix: strings.Trim(prefix, "/"),
	}
}

// errorLogEntry es el contenido JSON de cada log de error.
type errorLogEntry struct {
	RobotID     string `json:"robot_id"`
	ExecutionID string `json:"execution_id"`
	Timestamp   string `json:"timestamp"`
	Status      string `json:"status"`
	HttpCode    uint16 `json:"http_code"`
	DurationMs  uint64 `json:"duration_ms"`
	ErrorMsg    string `json:"error_message"`
}

func newErrorLogEntry(robotId, execID string, now time.Time, result models.ExecutionResult) errorLogEntry {
	return errorLogEntry{
		RobotID:     robotId,
		ExecutionID: execID,
		Timestamp:   now.Format(time.RFC3339),
		Status:      result.Status,
		HttpCode:    result.HttpCode,
		DurationMs:  result.DurationMs,
		ErrorMsg:    result.ErrorMsg,
	}
}

// objectKey arma la key: <prefix>/<robotId>/<YYYY>/<MM>/<DD>/<unixMillis>-<id>.<ext>
func (s *ErrorLogStore) objectKey(robotId string, t time.Time, id, ext string) string {
	return fmt.Sprintf("%s/%s/%s/%d-%s.%s", s.prefix, robotId, t.Format("2006/01/02"), t.UnixMilli(), id, ext)
}

func (s *ErrorLogStore) put(ctx context.Context, key string, body []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put object s3://%s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// SaveError sube de inmediato un objeto JSON con el log de una ejecucion
// fallida y retorna su key. Es la estrategia usada por la Lambda.
func (s *ErrorLogStore) SaveError(ctx context.Context, robotId string, now time.Time, result models.ExecutionResult) (string, error) {
	execID, err := newExecutionID()
	if err != nil {
		return "", fmt.Errorf("generar id de ejecucion: %w", err)
	}

	body, err := json.Marshal(newErrorLogEntry(robotId, execID, now, result))
	if err != nil {
		return "", fmt.Errorf("serializar log: %w", err)
	}

	key := s.objectKey(robotId, now, execID, "json")
	if err := s.put(ctx, key, body, "application/json"); err != nil {
		return "", err
	}

	return key, nil
}

// putBatch sube un lote como JSON Lines (un error por linea).
func (s *ErrorLogStore) putBatch(ctx context.Context, b *errorBatch) error {
	var buf bytes.Buffer
	for _, e := range b.entries {
		line, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("serializar log del lote: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return s.put(ctx, b.key, buf.Bytes(), "application/x-ndjson")
}

// newExecutionID genera un sufijo aleatorio corto para evitar colisiones de
// key cuando un mismo robot falla dos veces en el mismo milisegundo.
func newExecutionID() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
