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

	awsCfg, err := config.LoadDefaultConfig(context.TODO())

	if err != nil {
		panic(fmt.Sprintf("no se pudo cargar config de AWS: %v", err))
	}

	dbClient := dynamodb.NewFromConfig(awsCfg)

	repo := repository.NewRobotRepository(
		dbClient,
		cfg.TableName,
		cfg.GSIByClientName,
		time.Duration(cfg.LockTTL)*time.Second,
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
