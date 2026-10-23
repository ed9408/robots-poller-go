// Package config centraliza la configuracion de runtime de la aplicacion.
package config

import (
	"os"
	"strconv"
)

// Config contiene los parametros usados para conectar con AWS, llamar a los
// robots y limitar el paralelismo de una invocacion.
type Config struct {
	TableName              string
	GSIByClientName        string
	LockTTL                uint16 // segundos
	HttpTimeoutSec         uint8
	MaxConcurrency         uint8 // robots procesados en paralelo por invocación
	EC2PollIntervalSeconds uint8
}

// Load devuelve la configuracion efectiva. Los nombres de tabla, GSI y el
// paralelismo pueden cambiarse mediante variables de entorno; los TTL y el
// timeout HTTP tienen valores fijos definidos por la aplicacion.
func Load() Config {
	return Config{
		TableName:              getEnv("ROBOTS_TABLE_NAME", "robots"),
		GSIByClientName:        getEnv("GSI_BY_CLIENT_NAME", "gsi-by-client"),
		LockTTL:                120,
		HttpTimeoutSec:         30,
		MaxConcurrency:         getEnvUint8("MAX_CONCURRENCY", 5),
		EC2PollIntervalSeconds: getEnvUint8("EC2_POLL_INTERVAL_SECONDS", 5),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func getEnvUint8(key string, fallback uint8) uint8 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return uint8(n)
		}
	}
	return fallback
}

func getEnvUint16(key string, fallback uint16) uint16 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseUint(v, 10, 16); err == nil {
			return uint16(n)
		}
	}
	return fallback
}
