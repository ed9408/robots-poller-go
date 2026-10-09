// Package repository implementa el acceso a estados y locks de robots en
// DynamoDB, y el guardado de logs de error en S3.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"orchestrator/internal/models"
)

// RobotRepository contiene el cliente DynamoDB y la configuracion de la tabla
// usada para leer estados y registrar ejecuciones. Los logs de error ya no se
// guardan en DynamoDB sino en S3, a traves de logs (inmediato en Lambda, por
// lotes en EC2).
type RobotRepository struct {
	client      *dynamodb.Client
	tableName   string
	gsiByClient string
	lockTTL     time.Duration
	logs        ErrorLogger
}

// NewRobotRepository crea un repositorio para la tabla y el GSI indicados.
// lockTTL controla cuanto tiempo permanece vigente un lock y logs es quien
// guarda en S3 los logs de las ejecuciones con error.
func NewRobotRepository(client *dynamodb.Client, tableName string, gsiByClient string, lockTTL time.Duration, logs ErrorLogger) *RobotRepository {
	return &RobotRepository{
		client:      client,
		tableName:   tableName,
		gsiByClient: gsiByClient,
		lockTTL:     lockTTL,
		logs:        logs,
	}
}

func pk(robotId string) string {
	return fmt.Sprintf("ROBOT#%s", robotId)
}

// ListAllRobots trae todos los robots activos usando el GSI sparse
// gsi-active-robots, que solo indexa ítems STATE (nunca LOG), evitando
// leer el volumen de logs acumulados en la tabla.
func (r *RobotRepository) ListAllRobots(ctx context.Context) ([]models.RobotState, error) {
	keyCond := expression.Key("entity_type").Equal(expression.Value("ROBOT"))
	exprBuilder, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		return nil, fmt.Errorf("build query expression: %w", err)
	}

	var states []models.RobotState
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(r.tableName),
			IndexName:                 aws.String("gsi-active-robots"),
			KeyConditionExpression:    exprBuilder.KeyCondition(),
			ExpressionAttributeNames:  exprBuilder.Names(),
			ExpressionAttributeValues: exprBuilder.Values(),
			ExclusiveStartKey:         lastEvaluatedKey,
		})
		if err != nil {
			return nil, fmt.Errorf("query gsi-active-robots: %w", err)
		}

		var page []models.RobotState
		if err := attributevalue.UnmarshalListOfMaps(out.Items, &page); err != nil {
			return nil, fmt.Errorf("unmarshal robots: %w", err)
		}
		states = append(states, page...)

		if out.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = out.LastEvaluatedKey
	}

	return states, nil
}

// QueryByClient obtiene todos los ítems STATE de los robots pertenecientes
// a un cliente, usando el GSI gsi-by-client. Como el GSI es "sparse"
// (solo los ítems STATE tienen el atributo client_id), esto no trae
// ítems LOG por accidente.
func (r *RobotRepository) QueryByClient(ctx context.Context, clientId string) ([]models.RobotState, error) {
	keyCond := expression.Key("client_id").Equal(expression.Value(clientId))
	exprBuilder, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()

	if err != nil {
		return nil, fmt.Errorf("build query expression: %w", err)
	}

	var states []models.RobotState
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(r.tableName),
			IndexName:                 aws.String(r.gsiByClient),
			KeyConditionExpression:    exprBuilder.KeyCondition(),
			ExpressionAttributeNames:  exprBuilder.Names(),
			ExpressionAttributeValues: exprBuilder.Values(),
			ExclusiveStartKey:         lastEvaluatedKey,
		})

		if err != nil {
			return nil, fmt.Errorf("query by client %q: %w", clientId, err)
		}

		var page []models.RobotState

		if err := attributevalue.UnmarshalListOfMaps(out.Items, &page); err != nil {
			return nil, fmt.Errorf("unmarshal client robots: %w", err)
		}

		states = append(states, page...)

		if out.LastEvaluatedKey == nil {
			break
		}

		lastEvaluatedKey = out.LastEvaluatedKey
	}

	return states, nil
}

// GetState obtiene la configuración/estado de un robot individual.
// Se conserva para debugging y para reutilización desde EC2 (robots
// sub-minuto) que siguen operando robot por robot.
func (r *RobotRepository) GetState(ctx context.Context, robotId string) (*models.RobotState, error) {
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk(robotId)},
			"SK": &types.AttributeValueMemberS{Value: "STATE"},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("get state: %w", err)
	}

	if out.Item == nil {
		return nil, fmt.Errorf("no existe configuración para robot %q", robotId)
	}

	var state models.RobotState

	if err := attributevalue.UnmarshalMap(out.Item, &state); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}

	return &state, nil
}

// TryAcquireLock intenta tomar el lock del robot de forma optimista. Retorna
// false sin error si otro proceso ya tiene un lock vigente.
func (r *RobotRepository) TryAcquireLock(ctx context.Context, robotId string) (bool, error) {
	expiry := time.Now().Add(r.lockTTL).UTC().Format(time.RFC3339)
	now := time.Now().UTC().Format(time.RFC3339)

	cond := expression.Or(
		expression.AttributeNotExists(expression.Name("lock_status")),
		expression.Name("lock_status").Equal(expression.Value("IDLE")),
		expression.Name("lock_expires_at").LessThan(expression.Value(now)),
	)

	upd := expression.Set(expression.Name("lock_status"), expression.Value("RUNNING")).Set(expression.Name("lock_expires_at"), expression.Value(expiry))

	exprBuilder, err := expression.NewBuilder().WithCondition(cond).WithUpdate(upd).Build()

	if err != nil {
		return false, fmt.Errorf("build lock expression: %w", err)
	}

	_, err = r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk(robotId)},
			"SK": &types.AttributeValueMemberS{Value: "STATE"},
		},
		ConditionExpression:       exprBuilder.Condition(),
		UpdateExpression:          exprBuilder.Update(),
		ExpressionAttributeNames:  exprBuilder.Names(),
		ExpressionAttributeValues: exprBuilder.Values(),
	})

	if err != nil {
		var condFailed *types.ConditionalCheckFailedException

		if errors.As(err, &condFailed) {
			return false, nil
		}
		return false, fmt.Errorf("acquire lock: %w", err)
	}

	return true, nil
}

// ReleaseLock libera el lock del robot. El llamador lo ejecuta via defer y no
// propaga su error para no ocultar el error principal del flujo.
func (r *RobotRepository) ReleaseLock(ctx context.Context, robotId string) error {
	upd := expression.Set(expression.Name("lock_status"), expression.Value("IDLE"))
	exprBuilder, err := expression.NewBuilder().WithUpdate(upd).Build()

	if err != nil {
		return err
	}

	_, err = r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk(robotId)},
			"SK": &types.AttributeValueMemberS{Value: "STATE"},
		},
		UpdateExpression:          exprBuilder.Update(),
		ExpressionAttributeNames:  exprBuilder.Names(),
		ExpressionAttributeValues: exprBuilder.Values(),
	})

	return err
}

// RecordExecution actualiza el STATE de la ejecucion indicada. Solo cuando la
// ejecucion fallo guarda ademas un log en S3 y deja en el STATE una referencia
// (last_error_key) para localizarlo. Las ejecuciones correctas no generan log.
func (r *RobotRepository) RecordExecution(ctx context.Context, robotId string, result models.ExecutionResult) error {
	now := time.Now().UTC()

	var errorKey string
	var logErr error

	if result.Status == "FAILURE" {
		errorKey, logErr = r.logs.SaveError(ctx, robotId, now, result)
	}

	// El STATE se actualiza siempre, aunque falle el guardado en S3: asi
	// last_execution_at avanza y el robot no se reejecuta en bucle.
	if err := r.updateStateAfterExecution(ctx, robotId, now, result, errorKey); err != nil {
		return fmt.Errorf("update state: %w", err)
	}

	if logErr != nil {
		return fmt.Errorf("save error log: %w", logErr)
	}

	return nil
}

// maxStateErrorLen limita el mensaje de error copiado al STATE para que el
// item siga siendo liviano; el mensaje completo vive en el log de S3.
const maxStateErrorLen = 300

func (r *RobotRepository) updateStateAfterExecution(ctx context.Context, robotId string, now time.Time, result models.ExecutionResult, errorKey string) error {
	upd := expression.Set(expression.Name("last_execution_at"), expression.Value(now.Unix())).
		Set(expression.Name("last_status"), expression.Value(result.Status)).
		Set(expression.Name("last_duration_ms"), expression.Value(result.DurationMs)).
		Add(expression.Name("execution_count"), expression.Value(1))

	if result.LastId != nil {
		upd = upd.Set(
			expression.Name("ult_id"),
			expression.Value(*result.LastId),
		)
	}

	if result.Status == "FAILURE" {
		msg := result.ErrorMsg
		if len(msg) > maxStateErrorLen {
			msg = msg[:maxStateErrorLen]
		}

		upd = upd.Set(expression.Name("last_error_at"), expression.Value(now.Unix())).
			Set(expression.Name("last_error_message"), expression.Value(msg))

		if errorKey != "" {
			upd = upd.Set(expression.Name("last_error_key"), expression.Value(errorKey))
		}
	}

	exprBuilder, err := expression.NewBuilder().WithUpdate(upd).Build()

	if err != nil {
		return err
	}

	_, err = r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk(robotId)},
			"SK": &types.AttributeValueMemberS{Value: "STATE"},
		},
		UpdateExpression:          exprBuilder.Update(),
		ExpressionAttributeNames:  exprBuilder.Names(),
		ExpressionAttributeValues: exprBuilder.Values(),
	})

	return err
}
