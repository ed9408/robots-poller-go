# Orchestrator

Servicio en Go para coordinar la ejecucion periodica de robots asociados a un
cliente. Incluye dos procesos que comparten la logica de negocio:

- `lambda`: handler para AWS Lambda que recibe un evento con `client_id`.
- `ec2poller`: proceso de larga duracion para EC2 que ejecuta ciclos
   periodicos.

Ambos consultan los robots configurados en DynamoDB, ejecutan solo los que ya
estan debidos y registran el resultado de cada ejecucion.

## Arquitectura

El flujo principal es:

1. `cmd/lambda/main.go` carga la configuracion, crea el cliente DynamoDB y
   registra la funcion Lambda.
2. `internal/handler` consulta los robots del cliente, evalua `IsDue` y los
   procesa en paralelo con un limite de concurrencia.
3. Antes de cada llamada, el handler intenta adquirir un lock optimista en
   DynamoDB. Si otro proceso posee el lock, el robot queda como
   `SKIPPED_LOCKED`.
4. `internal/http-client` hace un `GET` al endpoint del robot. Una respuesta
   exitosa puede devolver `{ "ult_id": "..." }` para continuar el cursor en
   la siguiente ejecucion.
5. `internal/repository` actualiza el estado del robot y agrega un item de
   log con TTL.
6. `cmd/ec2poller/main.go` ejecuta el mismo flujo en ciclos controlados por
   `EC2_POLL_INTERVAL_SECONDS` y se apaga ordenadamente con `SIGTERM` o
   `SIGINT`.

Los comentarios GoDoc de cada paquete y funcion contienen los detalles de
implementacion y las reglas de error.

## Evento de entrada

El evento esperado es:

```json
{
  "client_id": "cliente-123"
}
```

La Lambda devuelve un resumen con el total de robots debidos, exitos,
fallos, robots omitidos por lock y el resultado individual de cada robot. La
invocacion devuelve error cuando al menos un robot falla, incluido un fallo al
persistir el resultado despues de una llamada HTTP exitosa.

## Configuracion

| Variable | Requerida | Valor por defecto | Uso |
| --- | --- | --- | --- |
| `ROBOTS_TABLE_NAME` | No | `robots` | Tabla DynamoDB |
| `GSI_BY_CLIENT_NAME` | No | `gsi-by-client` | Nombre del GSI por `client_id` |
| `MAX_CONCURRENCY` | No | `5` | Robots simultaneos por invocacion |
| `EC2_POLL_INTERVAL_SECONDS` | No | `5` | Intervalo entre ciclos del proceso EC2 |

El lock dura 120 segundos, los logs duran 30 dias y cada llamada HTTP tiene
un timeout de 30 segundos. Esos valores estan definidos en
`internal/config/config.go`.

## DynamoDB

La clave primaria de la tabla usa:

- `PK`: `ROBOT#<robot_id>`
- `SK`: `STATE` para el estado actual o `LOG#<timestamp>` para un log

Los items `STATE` deben incluir al menos `client_id`, `robot_id`,
`endpoint_url`, `interval_seconds` y `enabled`. El GSI configurado debe usar
`client_id` como clave de particion. Es sparse porque los items `LOG` no
incluyen ese atributo.

El estado mantiene tambien `last_execution_at`, `ult_id`, `lock_status` y los
campos de la ultima ejecucion. Los logs incluyen estado, codigo HTTP,
duracion, mensaje de error y `ttl`; DynamoDB TTL debe estar habilitado sobre
el atributo `ttl` para que la limpieza sea automatica.

## Desarrollo y validacion

Requisitos: Go 1.26 o posterior, credenciales de AWS disponibles para el
proceso que se ejecute y acceso a la tabla DynamoDB configurada.

Desde este directorio se pueden usar los siguientes comandos:

```bash
go test ./...
go vet ./...
make build-lambda
make package-lambda
make build-ec2
```

`go test ./...` se usa actualmente como comprobacion de compilacion porque no
hay pruebas automatizadas. Los binarios y el ZIP generados por `make` no se
versionan; las reglas estan en `.gitignore`.

## IAM y despliegue

El rol de ejecucion de la Lambda necesita permisos de lectura sobre el GSI y
de lectura/escritura sobre los items de la tabla, como minimo las acciones
`dynamodb:Query`, `dynamodb:GetItem`, `dynamodb:UpdateItem` y
`dynamodb:PutItem`. Tambien necesita los permisos habituales de logs de
CloudWatch.

El binario se compila para Linux y se empaqueta con el nombre `bootstrap`, que
es el entrypoint requerido por Lambda con runtime `provided.al2023`:

```bash
make build-lambda
make package-lambda
```

El resultado es `bootstrap.zip`, listo para cargar en Lambda. Para construir
el proceso de EC2 se usa:

```bash
make build-ec2
```

## Inicializacion del repositorio

Cuando el proyecto este listo para versionarse, desde este directorio:

```bash
git init
git add .
git status
git commit -m "chore: initial commit"
```

Revisa `git status` antes del primer commit para confirmar que solo se
incluyen el codigo fuente, la documentacion y los archivos de configuracion
del proyecto.
