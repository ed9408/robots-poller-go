// Package main construye y arranca la funcion Lambda que orquesta la
// ejecucion periodica de los robots de un cliente.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	appconfig "orchestrator/internal/config"
	"orchestrator/internal/handler"
	httpclient "orchestrator/internal/http-client"
	"orchestrator/internal/models"
	"orchestrator/internal/repository"
)

var h *handler.Handler

// init carga la configuracion y construye las dependencias compartidas por
// todas las invocaciones de la Lambda.
func init() {
	cfg := appconfig.Load()

	if cfg.LogsBucket == "" {
		panic("LOGS_BUCKET es obligatoria: bucket S3 para los logs de errores")
	}

	awsCfg, err := config.LoadDefaultConfig(context.TODO())

	if err != nil {
		panic(fmt.Sprintf("no se pudo cargar config de AWS: %v", err))
	}

	dbClient := dynamodb.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg)

	// En Lambda cada error se sube de inmediato a S3 (sin lotes): el entorno
	// de ejecucion puede congelarse o destruirse entre invocaciones, y un
	// buffer en memoria perderia errores.
	logStore := repository.NewErrorLogStore(s3Client, cfg.LogsBucket, cfg.LogsPrefix)

	repo := repository.NewRobotRepository(
		dbClient,
		cfg.TableName,
		cfg.GSIByClientName,
		time.Duration(cfg.LockTTL)*time.Second,
		logStore,
	)
	caller := httpclient.NewRobotCaller(time.Duration(cfg.HttpTimeoutSec) * time.Second)

	h = handler.New(repo, caller, cfg.MaxConcurrency, handler.StandardTier)
}

// lambdaHandler adapta el evento de Lambda al handler de la aplicacion.
func lambdaHandler(ctx context.Context, _ models.PollEvent) (models.PollExecutionSummary, error) {
	return h.Handle(ctx)
}

// main registra lambdaHandler con el runtime de AWS Lambda.
func main() {
	lambda.Start(lambdaHandler)
}
