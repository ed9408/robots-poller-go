package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

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
	s3Client := s3.NewFromConfig(awsCfg)

	// En EC2 los errores se agrupan en lotes por robot y se suben cada
	// LogsFlushIntervalSec (o al llegar a LogsBatchMaxEntries errores).
	logStore := repository.NewErrorLogStore(s3Client, cfg.LogsBucket, cfg.LogsPrefix)
	batchStore := repository.NewBatchingErrorLogStore(
		logStore,
		time.Duration(cfg.LogsFlushIntervalSec)*time.Second,
		int(cfg.LogsBatchMaxEntries),
	)

	repo := repository.NewRobotRepository(
		ddbClient,
		cfg.TableName,
		cfg.GSIByClientName,
		time.Duration(cfg.LockTTL)*time.Second,
		batchStore,
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

	// El worker de lotes usa su propio contexto: debe seguir vivo hasta que
	// termine el ultimo ciclo, porque ese ciclo todavia puede registrar
	// errores (por ejemplo, llamadas HTTP canceladas por el apagado).
	flushCtx, stopFlush := context.WithCancel(context.Background())
	flushDone := make(chan struct{})

	go func() {
		defer close(flushDone)
		batchStore.Run(flushCtx)
	}()

	log.Printf("ec2poller iniciado, tick cada %s", tickInterval)

	for {
		select {
		case <-ctx.Done():
			log.Println("señal de apagado recibida, cerrando ec2poller")
			stopFlush()
			<-flushDone // espera el volcado final de lotes a S3
			return

		case <-ticker.C:
			runCycle(ctx, h, cfg.HttpTimeoutSec)
		}
	}
}

func runCycle(ctx context.Context, h *handler.Handler, httpTimeoutSec uint8) {
	// El ciclo necesita al menos el timeout HTTP completo, más margen
	// para las operaciones de lock/registro (que ahora están desacopladas,
	// pero igual conviene que el ciclo no corte la llamada HTTP a mitad).
	budget := time.Duration(httpTimeoutSec)*time.Second + 20*time.Second
	cycleCtx, cancel := context.WithTimeout(ctx, budget)
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
