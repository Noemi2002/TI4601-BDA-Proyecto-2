# TI4601-BDA-Proyecto-2


---

### 1. Requisitos previos

- Git.
- Docker Engine y Docker Compose v2 (`docker compose`, no `docker-compose`).
- Docker Desktop con WSL2 puede utilizarse en Windows.
- Terminal con Bash para ejecutar `pruebas/p8_escenarios.sh` y `pruebas/p4_reinicio_buffer.sh` (Linux o WSL2).
- Recursos suficientes para tres instancias de MongoDB 8.0.4, FastAPI, el contenedor temporal de carga, el servicio Historial y Spark (un master y hasta dos workers con 2 GiB de memoria cada uno).


Verificar las instalaciones:

```bash
git --version
docker --version
docker compose version
docker info
```

Si `docker info` falla, iniciar Docker antes de continuar.

### 2. Clonar el repositorio

```bash
git clone https://github.com/Noemi2002/TI4601-BDA-Proyecto-2.git
cd TI4601-BDA-Proyecto-2
```

### 3. Estructura de archivos del subsistema

```text
TI4601-BDA-Proyecto-2/
├── docker-compose.yml
├── docker-compose.spark.yml
├── mongo/
│   ├── crear_colecciones.js
│   ├── crear_indices.js
│   └── probar_*.js
├── dataset/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── generar_datos.py
│   └── cargar_datos.py
├── datos/
│   ├── manifest.json
│   └── *.jsonl
├── servicios/
│   ├── registro/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── database.py
│   │   ├── main.py
│   │   ├── envios.py
│   │   ├── revisiones.py
│   │   ├── exportacion.py
│   │   ├── comandos.py
│   │   └── historial_cliente.py        # outbox durable hacia el Historial
│   ├── sesion/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── main.py
│   ├── gateway/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── main.py
│   ├── historial/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py
│   │   └── compactar.py
│   └── analitica/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── main.py
├── spark/
│   └── jobs/
│       ├── generar_eventos.py
│       ├── consultas.py
│       ├── validar_consultas.py
│       └── esperar_evento.py
├── simulador/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── simulador.py
│   ├── carga.py
│   └── usuarios_p6.txt
├── pruebas/
│   ├── p2_registro_interno.py
│   ├── p3_comandos_internos.py
│   ├── p4_indices.py
│   ├── p5_recuperacion_interna.py
│   ├── p6_contrato_registro.py
│   ├── p6_corrida.sh
│   ├── p6_docker_stats.sh
│   ├── p6_resumen.py
│   ├── p6_resumen_stats.py
│   ├── p8_medir.py
│   ├── p8_escenarios.sh
│   ├── p4_historial.py
│   ├── e2e_rechazo.py
│   ├── e2e_gateway_rechazo.py
│   ├── p4_reinicio_buffer.py
│   ├── p4_reinicio_buffer.sh
│   ├── p4_entrega_durable.sh
│   ├── p4_durable_transiciones.py
│   ├── p4_durable_verificar.py
│   ├── p4_compactacion.py
│   ├── reconciliar_eventos.py
│   └── p5_analitica_api.py
├── datalake/
└── evidence/
```

**Directorios principales:**

- `mongo/`: creación de colecciones, índices y ejemplos de documentos.
- `dataset/`: generación y carga reproducible de datos.
- `datos/`: archivos JSONL generados y manifiesto de integridad.
- `servicios/registro/`: estado operacional y persistencia MongoDB.
- `servicios/sesion/`: locks, sesiones y borradores sobre Valkey.
- `servicios/gateway/`: único punto de entrada HTTP público.
- `servicios/historial/`: recepción de eventos, escritura del datalake Parquet y compactación.
- `servicios/analitica/`: API REST que ejecuta Q1–Q5 con Spark sobre el datalake (solo lectura).
- `spark/jobs/`: generador del historial, consultas Q1–Q5 y su validación.
- `datalake/`: archivos Parquet (`eventos/mes=AAAA-MM/`) y resultados de las consultas; se genera al ejecutar el generador.
- `simulador/`: simulación funcional y generación de carga concurrente.
- `pruebas/`: pruebas internas, aceptación, carga y tolerancia a fallas.
- `evidence/`: resultados originales y resúmenes consolidados de las pruebas.

### 4. Instalación inicial en otra computadora

Los siguientes pasos se realizan únicamente cuando se utiliza un conjunto nuevo de volúmenes MongoDB.

Si ya se inicializó el replica set anteriormente, consultar la sección 5.

#### 4.1. Levantar los tres nodos MongoDB

Desde la raíz del repositorio:

```bash
docker compose config
docker compose up -d mongo-1 mongo-2 mongo-3
docker compose ps
```

Se deben iniciar los siguientes contenedores:

| Servicio | Contenedor | Puerto interno |
|---|---|---|
| `mongo-1` | `proyecto2-mongo-1` | 27017 |
| `mongo-2` | `proyecto2-mongo-2` | 27017 |
| `mongo-3` | `proyecto2-mongo-3` | 27017 |

Los tres deben aparecer como `healthy`.

**Nota:** que los contenedores estén saludables no significa que el replica set ya se encuentre configurado.

#### 4.2. Inicializar el replica set

Este paso se ejecuta únicamente en la primera instalación.

```bash
docker compose exec -T mongo-1 mongosh --quiet --eval '
rs.initiate({
  _id: "rs-plataforma",
  members: [
    { _id: 0, host: "mongo-1:27017" },
    { _id: 1, host: "mongo-2:27017" },
    { _id: 2, host: "mongo-3:27017" }
  ]
})
'
```

El conjunto se denomina `rs-plataforma`.

Comprobar sus miembros:

```bash
docker compose exec -T mongo-1 mongosh --quiet --eval '
printjson(rs.status().members.map(m => ({
  name: m.name,
  state: m.stateStr,
  health: m.health
})))
'
```

**Resultado esperado:**

- Un nodo `PRIMARY`.
- Dos nodos `SECONDARY`.
- Los tres nodos con `health: 1`.

El primario puede cambiar después de una elección; no se debe asumir que siempre será `mongo-1`.

Si aparece `already initialized`, no repetir `rs.initiate()`. Basta con comprobar `rs.status()`.

#### 4.3. Crear las cinco colecciones

Copiar el script al contenedor:

```bash
docker compose cp mongo/crear_colecciones.js \
  mongo-1:/tmp/crear_colecciones.js
```

Ejecutarlo utilizando la conexión al replica set:

```bash
docker compose exec -T mongo-1 mongosh --quiet \
  'mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/plataforma?replicaSet=rs-plataforma' \
  /tmp/crear_colecciones.js
```

La base de datos se denomina `plataforma`.

Contiene cinco colecciones:

| Colección | Responsabilidad |
|---|---|
| `clientes` | Organizaciones propietarias de los proyectos |
| `proyectos` | Configuración, taxonomía y modalidad |
| `batches` | Lotes de tareas asociados a proyectos |
| `tareas` | Archivos, asignaciones, estados y anotaciones |
| `anotadores` | Personas anotadoras, revisoras y equipos |

Los documentos utilizan identificadores UUID almacenados como cadenas.

#### 4.4. Crear los índices obligatorios

Copiar el script:

```bash
docker compose cp mongo/crear_indices.js \
  mongo-1:/tmp/crear_indices.js
```

Ejecutarlo:

```bash
docker compose exec -T mongo-1 mongosh --quiet \
  'mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/plataforma?replicaSet=rs-plataforma' \
  /tmp/crear_indices.js
```

Se implementaron los cuatro índices requeridos:

| Colección | Índice | Campos |
|---|---|---|
| `tareas` | `idx_tareas_proyecto_estado_prioridad` | `proyecto_id`, `estado`, `prioridad` |
| `tareas` | `idx_tareas_anotador_estado` | `asignacion.anotador_id`, `estado` |
| `tareas` | `idx_tareas_batch` | `batch_id` |
| `proyectos` | `idx_proyectos_cliente_estado` | `cliente_id`, `estado` |

El campo `prioridad` utiliza orden descendente.

Los índices fueron evaluados posteriormente con `explain("executionStats")`.

#### 4.5. Generar los datos operacionales

Construir el contenedor de carga:

```bash
docker compose build carga
```

Generar los archivos:

```bash
docker compose run --rm --no-deps carga \
  python dataset/generar_datos.py
```

Se genera el conjunto principal de datos:

| Colección | Cantidad |
|---|---:|
| Clientes | 60 |
| Proyectos activos | 120 |
| Batches | 600 |
| Anotadores | 300 |
| Tareas | 60 000 |
| **Total** | **61 080** |

La distribución de tareas es:

| Modalidad | Porcentaje |
|---|---:|
| Texto | 30 % |
| Imagen | 30 % |
| Video | 20 % |
| LiDAR | 20 % |

El generador utiliza una semilla determinista e identificadores UUID estables.

También genera un manifiesto de integridad para verificar los hashes SHA-256 de los archivos JSONL.

#### 4.6. Cargar los datos en MongoDB

```bash
docker compose run --rm --no-deps carga \
  python dataset/cargar_datos.py
```

La carga utiliza:

- PyMongo.
- Escrituras con `w: "majority"`.
- Operaciones por lotes.
- Validación del manifiesto SHA-256.
- Inserciones idempotentes mediante `$setOnInsert`.

Esto permite ejecutar nuevamente la carga sin duplicar los documentos del conjunto principal ni sobrescribir sus estados existentes.

#### 4.7. Construir y levantar los servicios de aplicación

Construir los servicios principales:

```bash
docker compose build registro sesion gateway
```

Levantar la infraestructura y los servicios internos:

```bash
docker compose up -d \
  mongo-1 mongo-2 mongo-3 \
  valkey registro sesion gateway
```

Verificar el estado:

```bash
docker compose ps
```

El único puerto que debe quedar publicado hacia el host es el del Gateway:

```text
http://localhost:8000
```

Los servicios internos utilizan únicamente la red Docker `plataforma-net`:

| Servicio | Puerto interno | Publicado al host |
|---|---:|---|
| `gateway` | 8000 | Sí, `8000:8000` |
| `registro` | 8001 | No |
| `sesion` | 8002 | No |
| `valkey` | 6379 | No |
| `mongo-1` | 27017 | No |
| `mongo-2` | 27017 | No |
| `mongo-3` | 27017 | No |

Comprobar el Gateway:

```bash
curl -s http://localhost:8000/salud | python3 -m json.tool
```

En condiciones normales el estado general debe ser `operativo`.

### 5. Iniciar y detener el sistema

#### Después de apagar o reiniciar la computadora

```bash
docker compose up -d \
  mongo-1 mongo-2 mongo-3 \
  valkey registro sesion gateway

docker compose ps
```

No es necesario volver a inicializar el replica set ni volver a cargar los documentos.

La información de MongoDB permanece en sus volúmenes y Valkey conserva la información persistida mediante AOF.

Spark y el Historial se inician y detienen con `docker-compose.spark.yml`, después de levantar el compose principal (sección 18).

#### Detener los servicios

```bash
docker compose stop gateway sesion registro valkey mongo-1 mongo-2 mongo-3
```

También se puede utilizar:

```bash
docker compose down
```

Este comando elimina los contenedores y la red, pero conserva los volúmenes.

**No ejecutar `docker compose down -v` si se desea conservar la base de datos y el estado persistente de Valkey.**

Tampoco eliminar manualmente los volúmenes persistentes del proyecto.

### 6. Arquitectura implementada

```text
                    Cliente / Simulador
                           |
                        HTTP :8000
                           |
                           v
                    +---------------+
                    |    Gateway    |
                    | FastAPI :8000 |
                    +---------------+
                      /           \
                     /             \
                    v               v
          +----------------+   +----------------+
          |    Registro    |   |     Sesión     |
          | FastAPI :8001  |   | FastAPI :8002  |
          +----------------+   +----------------+
                   |                    |
                   v                    v
          +----------------+      +-----------+
          | MongoDB 8.0.4  |      | Valkey 8  |
          | rs-plataforma  |      |   :6379   |
          +----------------+      +-----------+
             /      |      \
            v       v       v
        mongo-1  mongo-2  mongo-3
```

La comunicación se realiza mediante la red interna Docker `plataforma-net`.

El **Gateway es el único punto de entrada público** de la plataforma. El simulador no se comunica directamente con MongoDB, Valkey, Registro ni Sesión.

El servicio Registro mantiene el estado operacional definitivo de las tareas en MongoDB. No consulta Valkey y no escribe directamente archivos Parquet.

El servicio Sesión administra candados temporales, sesiones, borradores y actividad de edición en Valkey. Valkey no sustituye a MongoDB como fuente de verdad del estado definitivo.

La conexión de PyMongo utiliza los tres miembros del replica set:

```text
mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/?replicaSet=rs-plataforma
```

El **Historial** (`historial:8003`, red interna) recibe del Registro un evento por cada transición confirmada por MongoDB y lo publica en el datalake Parquet (`datalake/eventos/mes=AAAA-MM/`). La **Analítica** (`analitica:8004`, red interna) ejecuta Q1–Q5 con **Spark**, que lee únicamente esos archivos, en modo de solo lectura (volumen montado `:ro`); no consulta MongoDB ni el Historial. El Gateway expone `GET /analitica/{q1..q5}`. Historial, Spark y Analítica se levantan con `docker-compose.spark.yml` (sección 18).

```text
Registro --outbox (MongoDB)--POST /eventos--> Historial --micro-lote--> datalake/eventos/mes=AAAA-MM/*.parquet
                                                                          ^
Gateway --GET /analitica/{q}--> Analítica (driver Spark) --> Spark workers  | (solo lectura)
```

### 7. Servicio REST de registro

El servicio está implementado con FastAPI y expone seis rutas internas.

| Método | Endpoint | Función |
|---|---|---|
| GET | `/salud` | Verificar servicio y MongoDB |
| POST | `/tareas/siguiente` | Adquirir una tarea atómicamente |
| POST | `/tareas/{tarea_id}/enviar` | Enviar anotación |
| POST | `/tareas/{tarea_id}/revision` | Iniciar, aprobar o rechazar revisión |
| GET | `/proyectos/{proyecto_id}/exportar` | Exportar tareas aprobadas por páginas |
| POST | `/comandos` | Ejecutar operaciones complementarias |

FastAPI genera automáticamente el contrato OpenAPI en `/openapi.json`.

#### 7.1. Acciones de `/comandos`

El endpoint `POST /comandos` concentra operaciones complementarias para cumplir el límite de rutas internas.

Admite:

| Acción | Función |
|---|---|
| `crear_proyecto` | Registrar proyecto y taxonomía |
| `crear_lote` | Registrar lote y tareas |
| `liberar_tarea` | Liberar una asignación |
| `renovar_asignacion` | Extender el vencimiento |
| `consultar_vencidas` | Identificar asignaciones vencidas |
| `recuperar_vencida` | Recuperar una tarea vencida |

#### 7.2. Asignación atómica de tareas

Endpoint:

```text
POST /tareas/siguiente
```

Ejemplo de solicitud:

```json
{
  "proyecto_id": "UUID_DEL_PROYECTO",
  "anotador_id": "UUID_DEL_ANOTADOR",
  "token": "UUID_DEL_TOKEN"
}
```

La asignación utiliza `find_one_and_update()` de MongoDB.

Esto garantiza que una tarea disponible no sea asignada simultáneamente a dos anotadores.

Respuestas:

- HTTP 200: tarea asignada correctamente.
- HTTP 204: no existen tareas disponibles.
- HTTP 404: proyecto inexistente o inactivo.
- HTTP 403: anotador no autorizado.
- HTTP 503: MongoDB no responde o no confirma la operación.

La respuesta exitosa incluye `tarea_id`, `proyecto_id`, `batch_id`, `estado`, `version`, `archivo`, `asignacion` y `evento_id`.

#### 7.3. Envío de anotaciones

Endpoint:

```text
POST /tareas/{tarea_id}/enviar
```

Valida:

- Existencia de la tarea.
- Estado `abierta`.
- Identificador del anotador.
- Token de asignación.
- Fecha de vencimiento.
- Geometría compatible.
- Etiquetas permitidas.

Un envío correcto cambia el estado a `anotada` e incrementa la versión.

Un segundo envío o un token incorrecto devuelve HTTP 409.

#### 7.4. Revisión de anotaciones

Endpoint:

```text
POST /tareas/{tarea_id}/revision
```

Las operaciones son:

**Iniciar revisión:**

```json
{
  "revisor_id": "UUID_DEL_REVISOR",
  "accion": "iniciar"
}
```

**Aprobar:**

```json
{
  "revisor_id": "UUID_DEL_REVISOR",
  "accion": "aprobar"
}
```

**Rechazar:**

```json
{
  "revisor_id": "UUID_DEL_REVISOR",
  "accion": "rechazar",
  "codigo_motivo": "ETIQUETA_INCORRECTA"
}
```

El rechazo requiere un motivo y devuelve la tarea al estado `disponible`.

En el historial Parquet, el evento correspondiente se registra como `rechazada` (secciones 20 y 21).

#### 7.5. Exportación paginada

Endpoint:

```text
GET /proyectos/{proyecto_id}/exportar?pagina=1&limite=50
```

Devuelve únicamente tareas aprobadas.

La respuesta incluye:

- `proyecto_id`
- `pagina`
- `limite`
- `total`
- `total_paginas`
- `tareas`

#### 7.6. Recuperación de asignaciones vencidas

Se implementaron operaciones internas para:

1. Renovar asignaciones vigentes.
2. Consultar tareas cuya asignación venció.
3. Recuperar tareas vencidas.
4. Devolverlas a `disponible`.
5. Permitir que otro anotador las adquiera.

La recuperación verifica atómicamente el token, la versión, el estado y el vencimiento.

La integración con Valkey debe comprobar el estado del candado antes de solicitar esta recuperación.

### 8. Máquina de estados de las tareas

```text
disponible
    |
    | Asignación
    v
  abierta
    |
    | Envío
    v
  anotada
    |
    | Iniciar revisión
    v
en_revision
    |
    +------------------+
    |                  |
    | Aprobar          | Rechazar
    v                  v
 aprobada          disponible
```

Adicionalmente:

- Una tarea `abierta` puede liberarse y regresar a `disponible`.
- Una tarea vencida puede recuperarse y regresar a `disponible`.
- Una renovación válida extiende el vencimiento.
- Cada transición válida de estado incrementa el campo `version`.
- Las renovaciones no incrementan la versión.
- Las transiciones inválidas no deben modificar la tarea.

Los estados almacenados en MongoDB son:

```text
disponible
abierta
anotada
en_revision
aprobada
```

`rechazada` corresponde al evento del historial, no a un estado persistente de la tarea en MongoDB.

### 9. Pruebas internas del subsistema Registro

Las pruebas se ejecutan desde la raíz del repositorio.

#### 9.1. Concurrencia y transiciones del Registro (prueba interna)

Archivo:

```text
pruebas/p2_registro_interno.py
```

Ejecutar:

```bash
set -o pipefail

docker compose run --rm --no-deps carga \
  python pruebas/p2_registro_interno.py \
  2>&1 | tee evidence/p2-registro-interno.txt
```

Verifica:

- 50 solicitudes concurrentes.
- 10 tareas disponibles.
- Exactamente 10 asignaciones.
- 40 respuestas HTTP 204.
- Ninguna tarea con doble asignación.
- Token incorrecto devuelve HTTP 409.
- Primer envío devuelve HTTP 200.
- Segundo envío devuelve HTTP 409.
- Inicio de revisión.
- Aprobación y rechazo.
- Exportación paginada.

**La prueba interna de concurrencia y transiciones fue superada.** La validación integrada equivalente mediante el Gateway se mantiene separada de esta prueba interna.

#### 9.2. Comandos internos del Registro

Archivo:

```text
pruebas/p3_comandos_internos.py
```

Ejecutar:

```bash
docker compose run --rm --no-deps carga \
  python pruebas/p3_comandos_internos.py \
  2>&1 | tee evidence/p3-comandos-internos.txt
```

Verifica:

- Creación de proyectos sin duplicados.
- Creación de lotes y tareas.
- Idempotencia de las operaciones.
- Liberación de tareas.
- Rechazo de tokens incorrectos.
- Reasignación a otro anotador.

#### 9.3. Evaluación de índices

Archivo:

```text
pruebas/p4_indices.py
```

Ejecutar:

```bash
docker compose run --rm --no-deps carga \
  python pruebas/p4_indices.py \
  2>&1 | tee evidence/p4-indices.txt
```

Utiliza `explain("executionStats")` para comparar:

- Selección automática del índice.
- Índice forzado mediante `hint`.
- Recorrido sin índice mediante `$natural`.

Genera:

```text
evidence/p4_indices_resumen.json
evidence/p4_indices_explain.json
```

Los cuatro índices obligatorios fueron evaluados correctamente.

#### 9.4. Recuperación de tareas vencidas

Archivo:

```text
pruebas/p5_recuperacion_interna.py
```

Ejecutar:

```bash
docker compose run --rm --no-deps carga \
  python pruebas/p5_recuperacion_interna.py \
  2>&1 | tee evidence/p5-recuperacion-interna.txt
```

Verifica:

- Renovación correcta.
- Token incorrecto.
- Detección de vencimiento.
- Rechazo de envío después del vencimiento.
- Recuperación de tarea vencida.
- Control de versión.
- Prevención de recuperación duplicada.
- Reasignación a otro anotador.

Esta prueba pasó completamente.

#### 9.5. Contrato REST del Registro (prueba interna)

Archivo:

```text
pruebas/p6_contrato_registro.py
```

Ejecutar:

```bash
docker compose run --rm --no-deps carga \
  python pruebas/p6_contrato_registro.py \
  2>&1 | tee evidence/p6-contrato-registro.txt
```

Comprueba las seis rutas internas.

Exporta el contrato de OpenAPI:

```text
evidence/p6-registro-openapi.json
evidence/p6-contrato-registro.json
```

Este contrato puede utilizarse para implementar la integración con el gateway.

#### 9.6. P8 — Tolerancia a fallas de MongoDB

Archivos:

```text
pruebas/p8_medir.py
pruebas/p8_escenarios.sh
```

**Advertencia:** esta prueba detiene temporalmente nodos MongoDB. Ejecutarla únicamente cuando ningún otro integrante esté utilizando el conjunto.

Ejecutar:

```bash
set -o pipefail

bash pruebas/p8_escenarios.sh \
  2>&1 | tee evidence/p8-ejecucion.log
```

La prueba realiza:

1. Escrituras continuas con `w: "majority"`.
2. Caída de un nodo secundario.
3. Restauración del secundario.
4. Caída del nodo primario.
5. Elección de un nuevo primario.
6. Restauración del nodo anterior.
7. Caída de dos nodos para perder la mayoría.
8. Recuperación del replica set.
9. Verificación de escrituras confirmadas.
10. Comparación entre `w:1` y `w:"majority"`.

Consultar los reportes:

```bash
ID=$(cat evidence/p8_ultimo_id.txt)

cat "evidence/p8_reporte_${ID}.txt"
cat "evidence/p8_benchmark_resumen_${ID}.txt"
```

Al guardar los resultados, limpiar los documentos temporales:

```bash
ID=$(cat evidence/p8_ultimo_id.txt)

docker compose run --rm --no-deps carga \
  python pruebas/p8_medir.py limpiar --id "$ID"
```

Esta limpieza no elimina los documentos principales.

### 10. Resultados experimentales obtenidos

Resultados temporales obtenidos en las primeras pruebas

#### 10.1. Resultados P2

| Métrica | Resultado |
|---|---:|
| Solicitudes concurrentes | 50 |
| Tareas disponibles | 10 |
| Respuestas HTTP 200 | 10 |
| Respuestas HTTP 204 | 40 |
| Tareas asignadas dos veces | 0 |
| Latencia p50 | 163.39 ms |
| Latencia p95 | 175.49 ms |
| Latencia p99 | 187.71 ms |

También se verificaron correctamente:

- Token incorrecto: HTTP 409.
- Primer envío: HTTP 200.
- Segundo envío: HTTP 409.
- Aprobación duplicada: HTTP 409.
- Rechazo con motivo.
- Exportación paginada.

#### 10.2. Resultados P8

Identificador de ejecución:

```text
p8_20261112_171229
```

| Métrica | Resultado |
|---|---:|
| Intentos de escritura | 144 |
| Escrituras confirmadas por mayoría | 134 |
| Intentos sin confirmación | 10 |
| Escrituras confirmadas perdidas | 0 |
| Interrupción observada entre ACK | 0.621 s |
| Tiempo desde caída confirmada hasta primer ACK | 0.364 s |
| Escrituras confirmadas con secundario caído | 43 |
| Errores durante caída del secundario | 0 |
| Escrituras confirmadas sin mayoría | 0 |
| Intentos fallidos sin mayoría | 6 |

Durante la caída del primario `mongo-3`, se eligió a `mongo-1` como nuevo primario.

El **RPO observado fue cero**, porque ninguna escritura confirmada por mayoría desapareció después de restaurar el conjunto.

La interrupción de 0.621 segundos corresponde al intervalo observado entre confirmaciones de la sonda. No debe interpretarse como el tiempo interno exacto de elección de MongoDB.

#### 10.3. Comparación de write concern

Se ejecutaron 100 escrituras confirmadas por configuración.

| Write concern | p50 | p95 | p99 |
|---|---:|---:|---:|
| `w:1` | 0.31 ms | 0.50 ms | 0.56 ms |
| `w:"majority"` | 0.91 ms | 1.39 ms | 1.54 ms |

En este entorno, `w:"majority"` presentó una latencia superior a `w:1`, debido a que espera la confirmación de la mayoría.

Estas mediciones corresponden a contenedores ejecutados localmente. No pueden extrapolarse directamente al rendimiento de un clúster distribuido entre varias computadoras.

#### 10.4. Resultados de índices

Los cuatro índices fueron comprobados mediante `explain("executionStats")`.

Ejemplos:

**Consulta de tareas por lote:**

| Plan | Documentos examinados |
|---|---:|
| Con índice | 112 |
| Sin índice | 60 013 |

**Consulta de proyectos activos por cliente:**

| Plan | Documentos examinados |
|---|---:|
| Con índice | 2 |
| Sin índice | 121 |

MongoDB seleccionó automáticamente los índices correspondientes en estas consultas.

Los resultados demuestran una reducción del número de documentos examinados. Las latencias pueden variar según la computadora y el estado de la caché.

### 11. Estado de integración con los demás servicios

El Gateway, Registro, Sesión, Valkey y MongoDB se encuentran integrados para el flujo operacional de asignación, edición, borradores, envío, liberación, revisión, recuperación y exportación.

| Componente | Estado actual |
|---|---|
| Gateway → Registro | Integrado |
| Gateway → Sesión/Valkey | Integrado |
| Gateway público en `:8000` | Integrado y probado |
| Locks, sesiones y borradores | Integrados y probados |
| Renovación y vencimiento MongoDB/Valkey | Integrados y probados |
| Recuperación de tareas vencidas | Integrada y probada |
| Historial/Parquet (entrega durable con outbox, micro-lotes, compactación) | Integrado y probado (secciones 18 y 20) |
| Deduplicación histórica por `evento_id` | Completada en el Historial (sección 18) |
| Analítica/Spark Q1–Q5 (servicio REST) | Completada, validada y medida (secciones 19 y 20) |
| `/analitica/{q1..q5}` en Gateway | Integrado (sección 19.5) |
| Flujo distribuido completo con todos los componentes | Pendiente de validación final del equipo |

#### 11.1. Comunicación con el Gateway

El servicio Registro está disponible dentro de la red Docker en:

```text
http://registro:8001
```

El servicio Sesión está disponible en:

```text
http://sesion:8002
```

El Gateway consume estas direcciones internas y publica únicamente:

```text
http://localhost:8000
```

Las rutas públicas traducen y coordinan las operaciones necesarias entre Registro y Sesión sin exponer directamente los servicios internos.

#### 11.2. Coordinación con Valkey

MongoDB es la fuente de verdad del estado definitivo de las tareas.

Valkey administra los candados temporales, sesiones y borradores.

La integración implementada garantiza que:

- el token operacional coincida con el token de sesión;
- un candado no pueda adquirirse de nuevo mientras siga vigente;
- los candados se renueven durante la edición;
- los borradores se almacenen sin convertirse en anotaciones definitivas;
- una tarea vencida no se recupere mientras conserve un candado válido;
- después de una liberación o un envío exitoso se limpien las estructuras temporales correspondientes;
- el reinicio de Valkey preserve los borradores mediante AOF;
- una tarea cuyo candado expiró pueda recuperarse operacionalmente y volver a ofrecerse sin doble propiedad.

Esta coordinación fue validada mediante las pruebas de interrupción, recuperación y persistencia descritas más adelante.

#### 11.3. Integración con Parquet e Historial

El Registro genera el `evento_id` de cada transición y, **después de que MongoDB confirma**, guarda el evento en la colección `outbox_eventos` (`w:"majority"`). Un hilo de fondo lo envía al Historial mediante `POST /eventos`, en orden, y solo lo borra del outbox cuando el Historial responde `202` o `200` (duplicado). Si el Historial no responde, reintenta sin límite con espera creciente (0.5 s hasta 30 s) y los eventos pendientes sobreviven al reinicio del Registro. La respuesta HTTP al cliente nunca espera al Historial.

Se garantiza que:

- una transición confirmada genera su evento correspondiente (entrega al menos una vez);
- los eventos se identifican mediante `evento_id`;
- los duplicados se descartan en el Historial;
- un rechazo se registra como `rechazada` en Parquet;
- si el Historial cae, los eventos esperan en el outbox y se entregan al volver (sección 20.5).

Ventana residual: si el proceso del Registro muere entre el commit de la transición y la inserción en el outbox, el evento no se genera. `pruebas/reconciliar_eventos.py` la detecta comparando `ultimo_evento_id` de las tareas con el datalake (limitación declarada en la sección 22).

El Registro no escribe directamente archivos Parquet. El detalle del Historial está en la sección 18.

### 12. Solución de problemas

| Problema | Solución |
|---|---|
| Docker no responde | Comprobar `docker info` |
| MongoDB no está inicializado | Ejecutar `rs.initiate()` solamente en la primera instalación |
| `already initialized` | No repetir inicialización; utilizar `rs.status()` |
| No aparece un primario | Comprobar los tres contenedores y el replica set |
| Gateway no responde | Revisar `docker compose logs gateway` y `docker compose ps` |
| Gateway devuelve `degradado` | Verificar Registro y Sesión |
| Registro devuelve HTTP 503 | Revisar MongoDB y `docker compose logs registro` |
| Sesión falla | Revisar Valkey y `docker compose logs sesion` |
| HTTP 409 | Revisar estado, token, propietario, candado y versión |
| HTTP 204 | No existen tareas disponibles en el proyecto |
| No encuentra archivos JSONL | Ejecutar primero el generador |
| Error de hashes SHA-256 | Regenerar los datos y revisar el manifiesto |
| Cambios de Python no se aplican | Reconstruir el servicio modificado con `docker compose up -d --build <servicio>` |
| P8 se interrumpió | Restaurar los nodos afectados y verificar `rs.status()` |
| Una prueba falla después de reiniciar | Comprobar que todos los servicios requeridos estén levantados |
| `docker-compose.spark.yml` falla por la red | Levantar primero el compose principal: él crea la red `ti4601-proyecto2_plataforma-net` |
| Historial en `degradado` o 503 | MongoDB no responde; revisar `docker logs proyecto2-historial` |
| `/analitica/...` responde 503 o tarda mucho la primera vez | La primera consulta arranca la sesión de Spark (varios segundos); si falla, revisar `docker logs proyecto2-analitica` y que `spark-master` esté arriba |
| Hay eventos pendientes en `outbox_eventos` | El Historial no responde; al volver se entregan solos. Ver `docker logs proyecto2-registro` |
| Spark no ve eventos recién enviados | Esperar el micro-lote (hasta 5 s) o revisar `pendientes_en_buffer` en `/salud` del Historial |
| Tras regenerar el datalake el Historial deduplica mal | Reiniciar `historial` (carga los `evento_id` del lago al arrancar) |

Logs útiles:

```bash
docker compose logs --tail=100 gateway
docker compose logs --tail=100 sesion
docker compose logs --tail=100 registro
```

Verificar contenedores:

```bash
docker compose ps
```

Restaurar la infraestructura principal:

```bash
docker compose up -d mongo-1 mongo-2 mongo-3 valkey registro sesion gateway
```

### 13. Estado consolidado del subsistema Registro/MongoDB

| Apartado | Estado |
|---|---|
| Infraestructura Docker | Completado |
| Replica set MongoDB | Completado |
| Cinco colecciones y modelos | Completado |
| Cuatro índices obligatorios | Completado y evaluado |
| Generador y carga de datos | Completado |
| Servicio REST FastAPI Registro | Implementado y probado |
| Asignaciones atómicas y estados | Completado y probado |
| Revisión y exportación paginada | Completado |
| Prueba interna de concurrencia/transiciones | Superada |
| P8 de MongoDB | Superada |
| Contrato REST del Registro | Validado |
| Integración mediante Gateway | Completada |
| Coordinación de locks con Valkey | Completada |
| Recuperación coordinada de tareas vencidas | Completada |
| Entrega durable de eventos a Parquet | Completado (sección 18) |

La implementación del subsistema Registro/MongoDB está integrada con Gateway, Sesión/Valkey e Historial.

### 14. Gateway, sesiones y simulador HTTP

La plataforma utiliza un **Gateway FastAPI** como único punto de entrada público. El Gateway publica el puerto `8000` y enruta las solicitudes hacia los servicios internos de Registro y Sesión.

Los servicios internos no publican puertos hacia el host:

- `gateway`: puerto público `8000`;
- `registro`: puerto interno `8001`;
- `sesion`: puerto interno `8002`;
- `valkey`: puerto interno `6379`;
- `mongo-1`, `mongo-2`, `mongo-3`: puertos internos `27017`.

El simulador se comunica exclusivamente mediante HTTP con el Gateway y no utiliza clientes directos de MongoDB ni Valkey.

#### 14.1. Servicio de Sesión y Valkey

El servicio de Sesión utiliza Valkey 8 para almacenar información temporal asociada con la edición de tareas.

Valkey está configurado con persistencia AOF:

```text
appendonly yes
appendfsync everysec
```

Las estructuras utilizadas son:

```text
lock:tarea:{id}
sesion:{anotador}
borrador:{tarea}
editando:{proyecto}
```

Los candados utilizan:

```text
SET NX PX 120000
```

por lo que tienen una duración nominal de 120 segundos.

La renovación del candado, actualización del borrador y liberación utilizan operaciones atómicas controladas mediante token.

El servicio de Sesión expone internamente:

| Método | Endpoint | Función |
|---|---|---|
| GET | `/salud` | Estado del servicio y Valkey |
| POST | `/locks/adquirir` | Adquirir candado exclusivo |
| PUT | `/locks/renovar` | Renovar candado |
| DELETE | `/locks/liberar` | Liberar candado |
| PUT | `/borradores/{tarea_id}` | Guardar borrador y renovar sesión |
| POST | `/sesiones/recuperar` | Recuperar sesión, borrador o consultar candado |

Los borradores no modifican el estado definitivo de la tarea en MongoDB.

#### 14.2. Gateway público

El Gateway implementa actualmente:

| Método | Endpoint |
|---|---|
| GET | `/salud` |
| POST | `/proyectos` |
| POST | `/proyectos/{id}/lotes` |
| POST | `/tareas/siguiente` |
| PUT | `/tareas/{id}/borrador` |
| POST | `/tareas/{id}/enviar` |
| POST | `/tareas/{id}/liberar` |
| POST | `/tareas/{id}/revision` |
| POST | `/sesiones/recuperar` |
| GET | `/proyectos/{id}/exportar` |
| GET | `/analitica/{q1..q5}` |

`GET /analitica/{q1..q5}` entrega las consultas del servicio Analítica/Spark (sección 19.5).

##### 14.2.1. Asignación de tareas

`POST /tareas/siguiente`:

1. genera un token de sesión;
2. solicita una tarea al servicio Registro;
3. adquiere el candado correspondiente mediante Sesión/Valkey;
4. crea la sesión del anotador;
5. si la adquisición del candado falla, compensa liberando la asignación operacional;
6. antes de una nueva asignación puede recuperar tareas vencidas únicamente si no existe un candado válido.

##### 14.2.2. Borradores

`PUT /tareas/{id}/borrador`:

1. valida y renueva la asignación operacional;
2. guarda el último borrador en Valkey;
3. renueva el candado de la sesión;
4. no incrementa la versión de la tarea;
5. no genera una transición definitiva.

##### 14.2.3. Envío

`POST /tareas/{id}/enviar`:

1. verifica que exista un candado activo;
2. comprueba que el token coincida;
3. solicita al Registro la transición definitiva;
4. si MongoDB confirma la operación, limpia la sesión temporal;
5. un segundo envío de la misma tarea devuelve conflicto;
6. si el registro definitivo falla, el borrador permanece disponible para recuperación.

##### 14.2.4. Revisión

El Gateway permite:

```text
iniciar
aprobar
rechazar
```

Una aprobación lleva la tarea a `aprobada`.

Un rechazo devuelve la tarea operacional a `disponible`; el evento histórico correspondiente se registra como `rechazada` (sección 21).

#### 14.3. Endpoint de salud

`GET /salud` permanece accesible aunque fallen dependencias internas.

Los estados implementados son:

```text
operativo
degradado
caido
```

- Registro y Sesión disponibles: `operativo`.
- Una dependencia no disponible: `degradado`.
- Registro y Sesión no disponibles: `caido`.

### 15. Simulador HTTP

El directorio:

```text
simulador/
```

contiene un simulador de usuarios que se comunica únicamente con el Gateway mediante `httpx`.

Soporta los comandos:

```text
anotar
recuperar
```

Durante una edición normal:

1. solicita una tarea;
2. envía un borrador cada 2 segundos;
3. envía la anotación definitiva;
4. opcionalmente simula revisión y aprobación/rechazo.

También permite interrumpir una sesión antes del envío para comprobar recuperación y persistencia.

Para las pruebas de rendimiento se implementó adicionalmente:

```text
simulador/carga.py
```

que genera usuarios virtuales concurrentes y calcula latencias p50, p95 y p99.

### 16. Pruebas oficiales del Gateway, Sesión y simulador

#### 16.1. P1 — Gateway y salud

**Estado: COMPLETADA**

Se verificó:

- `/salud` en estado normal;
- estado `degradado` al detener una dependencia;
- estado `caido` al detener Registro y Sesión;
- el Gateway permaneció accesible durante las fallas;
- únicamente el Gateway publica un puerto hacia el host;
- Registro, Sesión, Valkey y MongoDB permanecen en la red interna;
- el simulador utiliza exclusivamente HTTP contra el Gateway.

Evidencias principales:

```text
evidence/p1_salud_normal.txt
evidence/p1_salud_degradado.txt
evidence/p1_salud_caido.txt
evidence/p1_puertos.txt
evidence/p1_simulador_aislamiento.txt
```

#### 16.2. P3 — Interrupción y recuperación de sesión

**Estado: COMPLETADA**

Se verificó:

1. edición con borradores periódicos;
2. interrupción del usuario;
3. recuperación del último borrador;
4. reinicio de Valkey;
5. persistencia del borrador mediante AOF;
6. conservación de la sesión después del reinicio;
7. expiración del candado;
8. recuperación operacional de la tarea vencida;
9. posibilidad de ofrecer nuevamente la tarea sin doble propiedad;
10. limpieza final de sesión, candado y borrador.

Evidencias:

```text
evidence/p3_interrupcion.txt
evidence/p3_recuperacion_inmediata.txt
evidence/p3_p8_reinicio_valkey.txt
evidence/p3_recuperacion_post_reinicio.txt
evidence/p3_lock_vencido.txt
evidence/p3_reasignacion.txt
evidence/p3_limpieza_final.txt
```

#### 16.3. P6 — Carga concurrente

**Estado: COMPLETADA**

La prueba se realizó con:

```text
10 usuarios
40 usuarios
100 usuarios
```

Para cada nivel se realizaron tres corridas.

Cada corrida utiliza:

```text
Warmup:                  30 s
Medición:                90 s
Intervalo de borrador:    2 s
```

Se registraron:

- p50 por endpoint;
- p95 por endpoint;
- p99 por endpoint;
- cantidad de muestras;
- errores;
- envíos por minuto;
- uso de CPU y memoria mediante `docker stats`.

##### 16.3.1. Resultados consolidados

| Usuarios | Envíos exitosos | Envíos/min promedio | Errores de asignación |
|---:|---:|---:|---:|
| 10 | 30/30 | 6.630 | 0 |
| 40 | 120/120 | 26.147 | 0 |
| 100 | 293/300 | 63.708 | 7 |

Latencias promedio de las tres corridas:

| Usuarios | Endpoint | p50 | p95 | p99 |
|---:|---|---:|---:|---:|
| 10 | siguiente | 292.870 ms | 300.912 ms | 300.912 ms |
| 10 | borrador | 148.677 ms | 219.112 ms | 266.443 ms |
| 10 | enviar | 191.543 ms | 202.947 ms | 202.947 ms |
| 40 | siguiente | 991.947 ms | 1038.764 ms | 1051.401 ms |
| 40 | borrador | 484.972 ms | 692.192 ms | 772.597 ms |
| 40 | enviar | 705.636 ms | 749.484 ms | 759.478 ms |
| 100 | siguiente | 1845.563 ms | 2422.906 ms | 2483.203 ms |
| 100 | borrador | 660.879 ms | 1309.929 ms | 1491.501 ms |
| 100 | enviar | 901.515 ms | 1241.760 ms | 1272.041 ms |

Con 10 y 40 usuarios no se observaron errores.

Con 100 usuarios se observaron siete fallos durante la adquisición de tareas:

```text
300 solicitudes de asignación
293 asignaciones exitosas
7 excepciones del cliente HTTP
97.67 % de éxito
2.33 % de error
```

Los usuarios que obtuvieron una tarea completaron correctamente las operaciones de borrador y envío.

La instrumentación utilizada durante estas corridas contabilizó las excepciones HTTP, pero no almacenó su tipo específico. Por ello, los siete errores no se atribuyen a una causa concreta sin evidencia adicional.

El throughput aumenta al incrementar la concurrencia, pero las latencias también crecen significativamente. A 100 usuarios aparece el primer nivel con fallos de adquisición.

Evidencias:

```text
evidence/p6_u10_r1.json
evidence/p6_u10_r2.json
evidence/p6_u10_r3.json

evidence/p6_u40_r1.json
evidence/p6_u40_r2.json
evidence/p6_u40_r3.json

evidence/p6_u100_r1.json
evidence/p6_u100_r2.json
evidence/p6_u100_r3.json

evidence/p6_u*_docker_stats.csv
evidence/p6_maquina.txt
evidence/p6_resumen_final.json
evidence/p6_recursos_resumen.json
```

##### 16.3.2. Análisis de recursos

Durante las nueve ejecuciones de P6 se capturaron periódicamente estadísticas de CPU y memoria mediante `docker stats`.

Las métricas fueron consolidadas por nivel de concurrencia utilizando las tres corridas de cada nivel.

| Servicio | CPU prom. 10 | CPU prom. 40 | CPU prom. 100 | Mem. prom. 10 | Mem. prom. 40 | Mem. prom. 100 |
|---|---:|---:|---:|---:|---:|---:|
| Gateway | 10.953 % | 36.349 % | 78.545 % | 98.071 MiB | 154.247 MiB | 209.603 MiB |
| Registro | 1.840 % | 5.266 % | 14.474 % | 38.360 MiB | 39.612 MiB | 40.436 MiB |
| Sesión | 4.049 % | 6.924 % | 13.585 % | 42.842 MiB | 43.978 MiB | 45.889 MiB |
| Valkey | 1.727 % | 1.916 % | 2.483 % | 5.272 MiB | 5.131 MiB | 4.925 MiB |
| Mongo-1 | 24.825 % | 25.443 % | 36.105 % | 98.483 MiB | 108.243 MiB | 120.518 MiB |
| Mongo-2 | 21.942 % | 22.812 % | 31.395 % | 87.206 MiB | 92.458 MiB | 104.457 MiB |
| Mongo-3 | 25.589 % | 22.575 % | 31.052 % | 102.547 MiB | 103.850 MiB | 118.154 MiB |

El componente que presentó el crecimiento más marcado al aumentar la concurrencia fue el Gateway.

Su CPU promedio evolucionó de:

```text
10 usuarios:   10.953 %
40 usuarios:   36.349 %
100 usuarios:  78.545 %
```

y su memoria promedio aumentó de:

```text
10 usuarios:    98.071 MiB
40 usuarios:   154.247 MiB
100 usuarios:  209.603 MiB
```

El máximo observado para el Gateway con 100 usuarios fue de `117.05 %` de CPU y `233.1 MiB` de memoria. En `docker stats`, un valor superior a 100 % puede ocurrir cuando un contenedor utiliza capacidad equivalente a más de un núcleo lógico.

Los tres nodos MongoDB también mantuvieron una carga de CPU significativa. Con 100 usuarios registraron promedios entre aproximadamente 31 % y 36 %, con máximos cercanos a 100 %.

Registro y Sesión incrementaron su utilización con la concurrencia, pero permanecieron considerablemente por debajo del Gateway.

Valkey fue el componente con menor utilización de recursos: con 100 usuarios presentó aproximadamente `2.483 %` de CPU promedio y menos de `5 MiB` de memoria promedio.

Las mediciones identifican al Gateway como el componente con mayor presión creciente de recursos. Esto coincide con el incremento de las latencias p95/p99 y con la aparición de errores de adquisición en el nivel de 100 usuarios, aunque las mediciones no permiten atribuir causalidad directa de dichos errores a un componente específico.

En conjunto, P6 muestra que el sistema incrementa su throughput al pasar de 10 a 40 y 100 usuarios, a costa de mayores latencias y, con 100 usuarios, una tasa de error de asignación aproximada de `2.33 %`.

Archivos consolidados:

```text
evidence/p6_resumen_final.json
evidence/p6_recursos_resumen.json
evidence/p6_maquina.txt
```

Estadísticas originales:

```text
evidence/p6_u10_r*_docker_stats.csv
evidence/p6_u40_r*_docker_stats.csv
evidence/p6_u100_r*_docker_stats.csv
```

El generador de carga se ejecutó como cliente de la plataforma y sus recursos no se incluyen en las estadísticas consolidadas de los servicios evaluados.

#### 16.4. P8 — Fallas de Valkey y sesiones

**Estado: COMPLETADA para el componente Valkey/Sesión**

Las pruebas de interrupción y recuperación demostraron adicionalmente:

- reinicio de Valkey;
- persistencia AOF;
- recuperación del borrador;
- conservación de información de sesión;
- expiración del candado;
- recuperación operacional de la tarea;
- ausencia de doble propiedad.

La tolerancia a fallas de MongoDB se evaluó por separado en la prueba P8 del subsistema Registro y también fue superada.

### 17. Estado consolidado del Gateway, Sesión y simulador

| Apartado | Estado |
|---|---|
| Gateway público `:8000` | Completado |
| Integración Gateway → Registro | Completada |
| Integración Gateway → Sesión/Valkey | Completada |
| Valkey con AOF `everysec` | Completado |
| Locks con TTL de 120 s | Completado |
| Sesiones y tokens | Completado |
| Renovación/heartbeat | Completado |
| Borradores recuperables | Completado |
| Liberación y limpieza temporal | Completado |
| Envío definitivo y protección contra doble envío | Completado |
| Revisión mediante Gateway | Completada |
| Recuperación de tareas vencidas | Completada |
| Simulador HTTP | Completado |
| P1 | Completada |
| P3 | Completada |
| P6 | Completada |
| P8 — componente Valkey/Sesión | Completada |
| `/analitica/{q1..q5}` | Completada (sección 19.5) |
| Entrega de eventos a Historial/Parquet | Completada (sección 18) |
| Validación final end-to-end con todos los subsistemas | Pendiente del equipo completo |

En el estado actual, el flujo operacional Gateway–Registro–Sesión–Valkey–MongoDB se encuentra implementado y probado, sus transiciones se registran en el datalake Parquet y se consultan con Spark. El pendiente restante es la validación final del sistema completo.

### 18. Historial y datalake Parquet

Servicio FastAPI (`servicios/historial/`), un solo proceso, puerto interno `8003` (no se publica al host). Se levanta con `docker-compose.spark.yml`, que se une a la red `plataforma-net` del compose principal. La única interfaz de administración publicada es la UI del master de Spark, limitada a `127.0.0.1:8080`.

| Método | Endpoint | Función |
|---|---|---|
| POST | `/eventos` | Recibe un evento: `202` aceptado, `200` duplicado, `422` inválido, `503` sin MongoDB ni caché |
| POST | `/flush` | Interno (pruebas): publica de inmediato el buffer |
| GET | `/salud` | Estado, MongoDB, buffer pendiente y `evento_id` conocidos |

**Contrato del evento:** `evento_id` (equivale a `event_id` del enunciado), `timestamp`, `mes`, `version`, `tarea_id`, `batch_id`, `proyecto_id`, `cliente_id`, `modalidad`, `tipo_tarea`, `equipo_id`, `anotador_id`, `revisor_id`, `estado_anterior`, `estado_nuevo`, `codigo_motivo`, `duracion_seg`, `toma_ts`, `sla_seg`.

**Mecanismos:**

- **Deduplicación por `evento_id`:** al arrancar carga los `evento_id` del lago y del WAL; un evento repetido responde `duplicado` y no se escribe.
- **Enriquecimiento:** completa `cliente_id`, `sla_seg` y `equipo_id` leyendo MongoDB (solo lectura, caché de 300 s).
- **Micro-lotes:** se publican cada 5 s o al llegar a 500 eventos.
- **Publicación atómica:** cada micro-lote se escribe en un archivo oculto `.tmp-*.parquet` y se publica con `os.replace` en `mes=AAAA-MM/evt-<fecha>-<id>.parquet`. Spark ignora los archivos que empiezan con `.`, por lo que no lee un archivo a medias.
- **Durabilidad:** cada evento aceptado se guarda en un WAL con `fsync` antes de responder `202`; tras una caída se recupera al arrancar. Del lado del Registro, el outbox en MongoDB evita perder eventos cuando el Historial no responde (sección 11.3).
- **Compactación:** `servicios/historial/compactar.py` reemplaza los archivos `evt-*` de una partición por uno solo `compact-*`, elimina filas repetidas por `evento_id` y lo publica de forma atómica antes de borrar los originales. No toca los `part-*` del generador. Uso: `docker exec proyecto2-historial python /app/compactar.py [--mes AAAA-MM] [--dry-run]`.
- **Estado de la fila:** el de MongoDB, salvo el rechazo: en MongoDB la tarea vuelve a `disponible` y en Parquet la fila es `rechazada`.

**Arranque** (el compose principal debe estar arriba):

```bash
docker compose up -d --build registro
docker compose -f docker-compose.spark.yml up -d spark-master spark-worker-1 historial analitica
docker compose -f docker-compose.spark.yml --profile dos-workers up -d spark-worker-2   # opcional
```

**Detención** (nunca `down -v`):

```bash
docker compose -f docker-compose.spark.yml stop analitica historial spark-worker-2 spark-worker-1 spark-master
```

### 19. Analítica con Apache Spark

Spark 3.5.3 standalone (master y 1 o 2 workers de 2 cores y 1 GiB por executor). Lee solo `datalake/eventos` y escribe los resultados en `datalake/resultados/q1..q5`.

#### 19.1. Generar el historial

```bash
docker exec proyecto2-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /opt/spark/jobs/generar_eventos.py \
  --tareas 175000 --dias 365 --hasta 2026-11-23
```

Resultado: **801 890 eventos** de 175 000 tareas, en 13 particiones `mes=` (2025-11 a 2026-11, extremos parciales). Con la semilla fija (4601) y los mismos parámetros se obtiene el mismo conjunto. Usa los clientes, proyectos, anotadores y equipos reales de `datos/*.jsonl`; los `tarea_id` son sintéticos. Reejecutarlo sobrescribe el datalake: **detener antes el Historial y reiniciarlo después**.

#### 19.2. Consultas

```bash
docker exec proyecto2-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /opt/spark/jobs/consultas.py \
  [--q q1 q3] [--repeticiones 20] [--explain] [--ref "AAAA-MM-DD HH:MM:SS"]
```

| Consulta | Definición implementada |
|---|---|
| Q1 | Aprobadas por día y modalidad, últimos 90 días. "Hoy" = evento más reciente del lago (se puede fijar con `--ref`) |
| Q2 | Aprobadas / revisadas por equipo (del anotador) y mes. "Revisadas" = eventos `aprobada` o `rechazada` |
| Q3 | Duración media y percentil 95 por modalidad y tipo de tarea |
| Q4 | Rechazadas / revisadas por cliente y proyecto (cuenta las filas `rechazada`) |
| Q5 | Tiempo entre la última toma y la aprobación: p50 y p95 por cliente y % que superó `sla_seg` |

**Validación:** `spark/jobs/validar_consultas.py` recalcula Q1–Q5 con pandas sobre el mismo Parquet; las cinco coinciden (`evidence/p5-validacion.txt`).

```bash
python3 -m venv .venv && . .venv/bin/activate && pip install pandas pyarrow
python3 spark/jobs/validar_consultas.py 2>&1 | tee evidence/p5-validacion.txt
```

#### 19.3. Planes de ejecución

(`evidence/p5-plan-q1.txt`, `evidence/p5-plan-q3.txt`)

- **Q1:** `PartitionFilters: mes >= 2026-08` (poda de particiones: solo agosto a noviembre), filtros empujados al lector de Parquet y 4 de 19 columnas leídas. Agrupa por día y modalidad (agregación parcial, `Exchange hashpartitioning`, agregación final) y ordena el resultado.
- **Q3:** sin filtro de partición (usa todo el historial), con poda de columnas y percentil exacto (`ObjectHashAggregate`). El `Sort` final ordena el resultado agregado (4 grupos), no los eventos.
- Los planes se imprimen antes de ejecutar (`isFinalPlan=false`); AQE ajusta las 200 particiones de shuffle en ejecución.

#### 19.4. Mediciones

Entorno en `evidence/entorno.txt`. Se hicieron 20 repeticiones por consulta; la primera (arranque de JVM, generación de código y metadatos de Parquet) se reporta aparte y los percentiles usan las otras 19 (con n=19, el p99 equivale casi al máximo).

| Consulta | 1 worker: 1.ª / p50 / p95 / p99 (s) | 2 workers: 1.ª / p50 / p95 / p99 (s) |
|---|---|---|
| Q1 | 2.26 / 0.468 / 0.723 / 0.964 | 3.14 / 0.507 / 1.389 / 1.485 |
| Q2 | 0.81 / 0.421 / 0.455 / 0.471 | 0.96 / 0.449 / 0.564 / 0.572 |
| Q3 | 1.25 / 0.391 / 0.471 / 0.489 | 1.36 / 0.424 / 0.558 / 0.737 |
| Q4 | 0.51 / 0.355 / 0.404 / 0.414 | 0.68 / 0.369 / 0.486 / 0.517 |
| Q5 | 1.24 / 0.786 / 0.873 / 0.901 | 1.79 / 0.770 / 0.953 / 0.987 |

Evidencias: `evidence/p5-consultas-1worker.txt` (`executors=1`), `evidence/p5-consultas-2workers.txt` y `evidence/p5-consultas-2workers-repeticion.txt` (`executors=2`).

**Conclusiones:**

- La primera ejecución cuesta entre 1.8 y 7 veces la mediana posterior.
- Con 2 workers no hay mejora: las diferencias de p50 (−2 % a +8 %) son del tamaño del ruido entre dos corridas idénticas con 2 executors (hasta ±8 %).
- Los dos workers comparten la máquina y el conjunto es pequeño, por lo que no se puede inferir cómo escalaría un clúster real.
- El conjunto ampliado (hasta 10 M de filas) no se midió por capacidad de la máquina y tiempo.

#### 19.5. API REST de analítica

Servicio FastAPI (`servicios/analitica/`, puerto interno `8004`, sin puertos publicados) con dos rutas internas: `GET /salud` y `GET /consultas/{q1..q5}`. El Gateway las expone como:

```text
GET /analitica/{q1..q5}?conjunto=principal|grande&ref=AAAA-MM-DD HH:MM:SS&limite=1000
```

- Ejecuta las mismas funciones `q1..q5` de `spark/jobs/consultas.py`, con el driver de Spark dentro del contenedor y los executors en los workers. Lee solo `datalake/eventos` (volumen `:ro`); no tiene acceso a MongoDB ni al Historial.
- La respuesta incluye `resultado`, `filas`, `tiempo_s` (consulta), `arranque_spark_s` (costo de crear la sesión; solo la primera consulta lo paga) y `ref`. La sesión se libera tras 120 s sin uso para no ocupar los cores del cluster.
- Errores: `404` consulta inexistente o conjunto no disponible (el conjunto `grande` no existe en este entorno), `422` parámetros inválidos, `503` si Spark no puede ejecutar la consulta.
- Mientras la sesión esté activa ocupa 2 cores del cluster: **detener `analitica` antes de medir 1 contra 2 workers**.

### 20. Pruebas P4 (Historial) y P5 (Analítica)

> `pruebas/p4_indices.py` y `pruebas/p5_recuperacion_interna.py` (sección 9) son pruebas internas del Registro; los P4 y P5 oficiales del enunciado son las de esta sección.

#### 20.1. P4: micro-lotes, deduplicación y publicación atómica

Se ejecuta con un Historial y un lago aparte (`eventos_p4`) para no mezclar eventos de prueba:

```bash
docker compose -f docker-compose.spark.yml run -d --no-deps --name hist-p4 \
  -e LAGO_DIR=/datalake/eventos_p4 -e FLUSH_SEGUNDOS=2 historial
docker exec -e LAGO_DIR=/datalake/eventos_p4 -e FLUSH_SEGUNDOS=2 -i hist-p4 python - \
  < pruebas/p4_historial.py 2>&1 | tee evidence/p4-historial.txt
docker rm -f hist-p4 && sudo rm -rf datalake/eventos_p4 datalake/eventos_p4_wal
```

**P4 superada** (`evidence/p4-historial.txt`): 20 eventos nuevos (`202`), reenvío `duplicado`, 10 envíos simultáneos del mismo evento con 1 aceptado, 21 filas en Parquet cada una una vez, enriquecimiento correcto, sin archivos temporales, evento de enero en `mes=2026-01`, rechazo guardado como `rechazada` con su motivo, micro-lote publicado en 2.1 s y `422` ante eventos inválidos.

#### 20.2. Rechazo de extremo a extremo y visibilidad en Spark

```bash
# Terminal A: Spark calentado, espera la señal
docker exec proyecto2-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /opt/spark/jobs/esperar_evento.py \
  | tee evidence/p4-visible-en-spark.txt
# Terminal B
docker exec -e SENAL=/datalake/_senal_e2e.json -i proyecto2-historial python - \
  < pruebas/e2e_rechazo.py 2>&1 | tee evidence/e2e-rechazo.txt
```

Resultado (`evidence/e2e-rechazo.txt`, `evidence/p4-visible-en-spark.txt`):

- Segundo envío de la misma tarea: **HTTP 409**.
- MongoDB tras el rechazo: `estado=disponible`, asignación vacía, versión 0 → 4.
- Parquet: `abierta → anotada → en_revision → rechazada`, sin ninguna fila `disponible`, con motivo, revisor y enriquecimiento correctos.
- Entre el HTTP 200 del rechazo y el evento visible: **2.9 s** en Parquet y **3.7 s** en Spark (Spark ya calentado; la última consulta duró 0.9 s). Es **una sola muestra**; el límite lo fija el micro-lote (≤ 5 s) más la consulta.
- Esta prueba se hizo directamente contra el Registro (`registro:8001`).

La misma prueba **entrando por el Gateway** (`http://gateway:8000`, con el lock en Valkey):

```bash
docker exec -i proyecto2-historial python - < pruebas/e2e_gateway_rechazo.py \
  2>&1 | tee evidence/e2e-gateway-rechazo.txt
```

Resultado (`evidence/e2e-gateway-rechazo.txt`): segundo envío **HTTP 409** (lo decide la validación del lock en Valkey; sin Gateway lo decide la actualización condicional de MongoDB), MongoDB `disponible` con versión 12 → 16, y las 4 filas en Parquet visibles **5.6 s** después del HTTP 200 del rechazo (una sola muestra). La prueba solo cuenta las filas con versión posterior a la inicial, porque el Gateway puede reasignar una tarea que ya tenía historial.

#### 20.3. Caída del Historial con eventos en buffer

```bash
bash pruebas/p4_reinicio_buffer.sh 2>&1 | tee evidence/p4-reinicio-historial.txt
```

Envía 20 eventos, mata el contenedor (`docker kill`) antes de que se publiquen y lo reinicia. Resultado: los 20 se recuperan del WAL, aparecen **exactamente una vez** en Parquet, el reenvío responde `duplicado` y no quedan archivos temporales.

#### 20.5. Entrega durable de eventos (Historial caído)

```bash
bash pruebas/p4_entrega_durable.sh 2>&1 | tee evidence/p4-entrega-durable.txt
```

Detiene el Historial, genera 4 transiciones reales por el Registro (el Registro confirma con HTTP 200), comprueba que los 4 eventos quedan en `outbox_eventos`, reinicia el propio Registro, levanta el Historial y verifica que los 4 eventos llegan a Parquet exactamente una vez y en orden, con el outbox vacío.

#### 20.6. Compactación

```bash
docker compose -f docker-compose.spark.yml run -d --no-deps --name hist-p4 \
  -e LAGO_DIR=/datalake/eventos_p4 -e FLUSH_SEGUNDOS=60 historial
docker exec -e LAGO_DIR=/datalake/eventos_p4 -i hist-p4 python - \
  < pruebas/p4_compactacion.py 2>&1 | tee evidence/p4-compactacion.txt
docker rm -f hist-p4 && sudo rm -rf datalake/eventos_p4 datalake/eventos_p4_wal
```

30 micro-lotes (30 archivos) más una copia duplicada se compactan en 1 archivo con 150 filas distintas, sin temporales, y el Historial sigue deduplicando.

#### 20.7. API de analítica por el Gateway

```bash
python3 pruebas/p5_analitica_api.py 2>&1 | tee evidence/p5-analitica-api.txt
```

Llama `GET /analitica/q1..q5` por `localhost:8000`, informa el costo de arranque de Spark (primera consulta) y los tiempos posteriores, compara la cantidad de filas con los CSV de `spark-submit` y comprueba los códigos 404/422. Q1 depende de `ref` (ventana de 90 días respecto al evento más reciente del lago): la prueba fija `REF_Q1` (por defecto `2026-11-23 23:58:45`, el corte con el que se generaron los CSV) para que la comparación sea exacta; si se regeneran los CSV, ejecutar con `REF_Q1="<ref impreso por consultas.py>"`.

#### 20.4. P5: consultas

Resultados validados contra pandas, planes de Q1 y Q3 y mediciones en las secciones 19.2 a 19.4.

### 21. Recorrido de una tarea y dónde vive cada dato

| Dato | Dónde | Por qué |
|---|---|---|
| Estado vigente, versión, asignación, `ultimo_evento_id` | MongoDB | Fuente de verdad del estado |
| Lock (`lock:tarea:{id}`, 120 s), sesión, borrador | Valkey | Temporales; no deciden el estado definitivo |
| Una fila por transición (inmutable) | Parquet | Historial para analítica |

1. `POST /tareas/siguiente`: MongoDB asigna la tarea (`abierta`), Valkey guarda lock y sesión y se emite el evento `abierta`.
2. `PUT /tareas/{id}/borrador` cada 2 s: solo Valkey; no genera evento ni cambia la versión.
3. `POST /tareas/{id}/enviar`: si MongoDB confirma, HTTP 200 y `anotada`; después se emite el evento.
4. Revisión: `en_revision` y luego `aprobada` o rechazo.

**Por qué un segundo envío da HTTP 409:** después del primer envío la tarea está `anotada`, no `abierta`. La actualización condicional de MongoDB (estado `abierta`, mismo anotador, mismo token, no vencida) ya no encuentra el documento. Lo decide MongoDB, no Valkey, y no se escribe ninguna fila en el datalake.

**Por qué el rechazo es `disponible` en MongoDB y `rechazada` en Parquet:** MongoDB guarda el estado de trabajo y la tarea debe volver a la cola; Parquet guarda lo que ocurrió, y Q4 cuenta las filas `rechazada`. Si el lago guardara `disponible`, el rechazo se perdería.

**Fallas:** el evento se emite solo después de que MongoDB confirma, así que Parquet no recibe eventos de escrituras no confirmadas. Con un nodo MongoDB caído quedan 2 de 3 (mayoría) y se sigue escribiendo; con dos caídos no hay mayoría y el Registro responde 503 (sección 10.2). Si el Historial cae, el WAL conserva lo aceptado (20.3); si MongoDB cae, el Historial usa su caché de 300 s o responde 503 y el Registro reintenta con el mismo `evento_id`.

### 22. Limitaciones

- **Una sola máquina:** los tres nodos de MongoDB y los workers de Spark comparten CPU, disco y red virtual; no hay latencia de red real ni fallas independientes.
- **`w:1` vs `w:"majority"`:** la diferencia medida (p50 0.31 ms contra 0.91 ms) es costo local y no se puede extrapolar a un clúster distribuido.
- **Spark:** 1 contra 2 workers no es una prueba de escalabilidad (mismo hardware, ~800 000 filas); el conjunto ampliado de 10 M de filas no se midió.
- **Entrega al menos una vez:** el outbox evita perder eventos si el Historial cae o el Registro se reinicia. Persiste una ventana residual: si el Registro muere entre el commit en MongoDB y la inserción en el outbox, el evento no se genera. `reconciliar_eventos.py` solo detecta el último evento de cada tarea, no los intermedios, y no lo reenvía.
- **Un solo proceso Historial:** el conjunto de `evento_id` vive en memoria (801 890 ids) y no escala horizontalmente.
- **Compactación manual:** `compactar.py` se ejecuta bajo demanda, no de forma programada. Entre publicar el archivo compactado y borrar los originales (milisegundos) un lector podría ver eventos duplicados, por lo que conviene ejecutarlo sin consultas en curso.
- **Deduplicación en el Historial:** se aplica al escribir; las consultas Spark leen el lago tal cual.
- **Datos:** 175 000 tareas en lugar de 100 000 de referencia para llegar a ~800 000 eventos en 12 meses, con particiones extremas parciales. Los eventos reales de la prueba e2e se agregaron al mismo lago.
- **Analítica:** el servicio ejecuta una consulta Spark a la vez y la primera tras 120 s de inactividad paga el arranque de Spark; el conjunto ampliado (`conjunto=grande`) responde 404 porque no se generó.
- **Tiempos de la prueba de rechazo:** 2.9 s (por el Registro) y 5.6 s (por el Gateway) son una sola muestra cada uno y dependen del micro-lote de hasta 5 s.