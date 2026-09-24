package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	appconfig "orchestrator/internal/config"
	"orchestrator/internal/handler"
	httpclient "orchestrator/internal/http-client"
	"orchestrator/internal/repository"
)

func main() {
	cfg := appconfig.Load()

	awsCfg, err := config.LoadDefaultConfig(context.Background())

	if err != nil {
		log.Fatalf("no se pudo cargar config de AWS: %v", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg)
	repo := repository.NewRobotRepository(
		ddbClient,
		cfg.TableName,
		cfg.GSIByClientName,
		time.Duration(cfg.LockTTL)*time.Second,
		time.Duration(cfg.LogTTLDays)*24*time.Hour,
	)

	caller := httpclient.NewRobotCaller(time.Duration(cfg.HttpTimeoutSec) * time.Second)
	h := handler.New(repo, caller, cfg.MaxConcurrency, handler.FastTier)

	tickInterval := time.Duration(cfg.EC2PollIntervalSeconds) * time.Second
	ticker := time.NewTicker(tickInterval)

	defer ticker.Stop()

	// Manejo de apagado ordenado: systemd/EC2 mandan SIGTERM al detener
	// el servicio, y queremos terminar el ciclo en curso antes de salir.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log.Printf("ec2poller iniciado, tick cada %s", tickInterval)

	for {
		select {
		case <-ctx.Done():
			log.Println("señal de apagado recibida, cerrando ec2poller")
			return

		case <-ticker.C:
			runCycle(ctx, h)
		}
	}
}

func runCycle(ctx context.Context, h *handler.Handler) {
	cycleCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	summary, err := h.Handle(cycleCtx)
	if err != nil {
		log.Printf("ciclo con errores: %v (exitosos=%d fallidos=%d omitidos=%d)",
			err, summary.Succeeded, summary.Failed, summary.Skipped)
		return
	}

	if summary.TotalDue > 0 {
		log.Printf("ciclo ok: %d robots procesados (exitosos=%d omitidos=%d)",
			summary.TotalDue, summary.Succeeded, summary.Skipped)
	}
}
