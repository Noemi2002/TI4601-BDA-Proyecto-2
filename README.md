# TI4601-BDA-Proyecto-2


---

### 1. Requisitos previos

- Git.
- Docker Engine y Docker Compose v2 (`docker compose`, no `docker-compose`).
- Docker Desktop con WSL2 puede utilizarse en Windows.
- Terminal con Bash para ejecutar `pruebas/p8_escenarios.sh` (Linux o WSL2).
- Recursos suficientes para tres instancias de MongoDB 8.0.4, FastAPI y el contenedor temporal de carga.


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
│   │   └── comandos.py
│   ├── sesion/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── main.py
│   └── gateway/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── main.py
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
│   └── p8_escenarios.sh
└── evidence/
```

**Directorios principales:**

- `mongo/`: creación de colecciones, índices y ejemplos de documentos.
- `dataset/`: generación y carga reproducible de datos.
- `datos/`: archivos JSONL generados y manifiesto de integridad.
- `servicios/registro/`: estado operacional y persistencia MongoDB.
- `servicios/sesion/`: locks, sesiones y borradores sobre Valkey.
- `servicios/gateway/`: único punto de entrada HTTP público.
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

Los subsistemas de Historial/Parquet y Analítica/Spark se integran de forma independiente. Las rutas analíticas públicas del Gateway se habilitan cuando el servicio analítico del equipo se encuentre disponible.

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

En el historial Parquet, el evento correspondiente debe registrarse como `rechazada`. Esta entrega histórica aún debe integrarse.

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
| Historial/Parquet | Pendiente de integración final con el componente correspondiente |
| Deduplicación histórica por `evento_id` | Pendiente del componente Historial |
| Analítica/Spark Q1–Q5 | Pendiente del componente Analítica |
| `/analitica/{q1..q5}` en Gateway | Pendiente de conectar cuando esté disponible Analítica |
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

El Registro genera identificadores de eventos para las transiciones.

La entrega durable de dichos eventos corresponde al servicio de Historial/Parquet y continúa como integración pendiente del equipo.

Debe garantizarse que:

- una transición confirmada genere su evento correspondiente;
- los eventos puedan identificarse mediante `evento_id`;
- los duplicados se manejen correctamente;
- un rechazo se registre como `rechazada` en Parquet;
- las fallas entre MongoDB y el historial puedan detectarse y reconciliarse.

El Registro no escribe directamente archivos Parquet.

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
| Entrega durable de eventos a Parquet | Pendiente del componente Historial |

La implementación del subsistema Registro/MongoDB está integrada con Gateway y Sesión/Valkey. Los pendientes corresponden a los componentes históricos y analíticos del sistema completo.

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

La integración de `/analitica/{q1..q5}` permanece pendiente del servicio Analítica/Spark.

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

Un rechazo devuelve la tarea operacional a `disponible`; el evento histórico correspondiente debe registrarse como `rechazada`.

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
| `/analitica/{q1..q5}` | Pendiente del servicio Analítica |
| Entrega de eventos a Historial/Parquet | Pendiente del servicio Historial |
| Validación final end-to-end con todos los subsistemas | Pendiente del equipo completo |

En el estado actual, el flujo operacional Gateway–Registro–Sesión–Valkey–MongoDB se encuentra implementado y probado. Los pendientes restantes corresponden a la integración de Historial/Parquet, Analítica/Spark y a la validación final del sistema completo.