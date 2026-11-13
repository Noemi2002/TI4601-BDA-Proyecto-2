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
│   └── registro/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── database.py
│       ├── main.py
│       ├── envios.py
│       ├── revisiones.py
│       ├── exportacion.py
│       └── comandos.py
├── pruebas/
│   ├── p2_registro_interno.py
│   ├── p3_comandos_internos.py
│   ├── p4_indices.py
│   ├── p5_recuperacion_interna.py
│   ├── p6_contrato_registro.py
│   ├── p8_medir.py
│   └── p8_escenarios.sh
└── evidence/
```

**Directorios principales:**

- `mongo/`: creación de colecciones, índices y ejemplos de documentos.
- `dataset/`: generación y carga reproducible de datos.
- `datos/`: archivos JSONL generados y manifiesto de integridad.
- `servicios/registro/`: implementación del servicio REST FastAPI.
- `pruebas/`: pruebas de concurrencia, recuperación, índices, contratos y fallas.
- `evidence/`: resultados originales de las pruebas.

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

#### 4.7. Construir y levantar FastAPI

```bash
docker compose up -d --build registro
docker compose ps
```

El servicio `registro` utiliza el puerto interno `8001`.

No publica ese puerto en el host porque el gateway debe ser el único punto de entrada público de la plataforma.

Comprobar el servicio:

```bash
docker compose exec registro python -c \
"import urllib.request; print(urllib.request.urlopen('http://localhost:8001/salud').read().decode())"
```

**Resultado esperado:**

```json
{
  "servicio": "registro",
  "estado": "operativo",
  "mongodb": "disponible"
}
```

### 5. Iniciar y detener el sistema

#### Después de apagar o reiniciar la computadora

```bash
docker compose up -d mongo-1 mongo-2 mongo-3 registro
docker compose ps
```

No es necesario volver a inicializar el replica set ni cargar los documentos.

La información permanece en los volúmenes de Docker.

#### Detener los servicios

```bash
docker compose stop registro mongo-1 mongo-2 mongo-3
```

También se puede utilizar:

```bash
docker compose down
```

Este último comando elimina los contenedores y la red, pero conserva los volúmenes.

**No ejecutar `docker compose down -v` si se desea conservar la base de datos.**

Tampoco eliminar manualmente los volúmenes:

- `mongo1_data`
- `mongo2_data`
- `mongo3_data`

### 6. Arquitectura implementada

```text
             Gateway
     (integración pendiente)
       Puerto público 8000
               |
               v
         Registro REST
         FastAPI :8001
               |
               v
        MongoDB 8.0.4
          rs-plataforma
               |
      +--------+--------+
      |        |        |
   mongo-1  mongo-2  mongo-3
```

La comunicación se realiza mediante la red interna de Docker denominada `plataforma-net`.

La conexión de PyMongo utiliza los tres miembros:

```text
mongodb://mongo-1:27017,mongo-2:27017,mongo-3:27017/?replicaSet=rs-plataforma
```

El servicio de registro se encarga exclusivamente de los datos operacionales.

No consulta directamente Valkey, no escribe archivos Parquet y no ejecuta consultas analíticas de Spark.

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

### 9. Pruebas implementadas

Las pruebas se ejecutan desde la raíz del repositorio.

#### 9.1. P2: concurrencia y transiciones

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

**P2 fue aprobada internamente.** La prueba definitiva mediante el gateway queda pendiente de integración.

#### 9.2. P3: comandos internos

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

#### 9.3. P4: evaluación de índices

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

#### 9.4. P5: recuperación de tareas vencidas

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

#### 9.5. P6: contrato del servicio REST

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

#### 9.6. P8: tolerancia a fallas de MongoDB

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

### 11. Integración pendiente con los demás servicios

La implementación interna del registro está terminada y probada.

Sin embargo, todavía debe integrarse con los otros componentes de la plataforma.

| Componente | Trabajo pendiente |
|---|---|
| Gateway | Consumir las seis rutas internas del registro |
| Gateway | Publicar las rutas solicitadas en el puerto 8000 |
| Gateway | Ejecutar P2 completa mediante las rutas públicas |
| Sesión/Valkey | Administrar locks, sesiones y borradores |
| Sesión/Valkey | Coordinar renovación y vencimiento con MongoDB |
| Historial/Parquet | Registrar eventos por cada transición |
| Historial/Parquet | Deduplicar eventos utilizando `evento_id` |
| Analítica/Spark | Ejecutar consultas históricas Q1–Q5 |
| Equipo completo | Validar el flujo distribuido y sus fallas |

#### 11.1. Comunicación con el gateway

El servicio de registro se encuentra disponible dentro de la red Docker en:

```text
http://registro:8001
```

El gateway debe utilizar esta dirección y no `localhost:8001`, porque ambos servicios se ejecutan en contenedores diferentes.

El gateway será el único servicio expuesto al host:

```text
http://localhost:8000
```

Las rutas públicas deben traducirse a las rutas internas del registro.

#### 11.2. Coordinación con Valkey

MongoDB es la fuente de verdad del estado definitivo de las tareas.

Valkey administra los candados temporales, sesiones y borradores.

La integración debe garantizar que:

- El token de MongoDB coincida con el token de sesión.
- No se mantengan locks huérfanos después de fallas.
- Los candados se renueven mediante latidos.
- No se recupere una tarea mientras conserve un lock válido.
- Un borrador no se confunda con una anotación confirmada.

La implementación actual dispone de las operaciones necesarias para renovar, consultar vencidas y recuperar tareas, pero la coordinación con Valkey todavía debe probarse.

#### 11.3. Integración con Parquet

El registro genera identificadores de eventos para las transiciones.

Sin embargo, la escritura de esos eventos en Parquet corresponde al servicio de historial.

Debe garantizarse que:

- Una transición confirmada genere su evento correspondiente.
- Los eventos puedan identificarse por `evento_id`.
- Los duplicados se manejen correctamente.
- Un rechazo se registre como `rechazada` en Parquet.
- Las fallas entre MongoDB y el historial puedan detectarse y reconciliarse.

La implementación interna del registro no garantiza todavía la entrega durable de eventos al historial.

### 12. Solución de problemas

| Problema | Solución |
|---|---|
| Docker no responde | Comprobar `docker info` |
| MongoDB no está inicializado | Ejecutar `rs.initiate()` solamente en la primera instalación |
| `already initialized` | No repetir inicialización; utilizar `rs.status()` |
| No aparece un primario | Comprobar los tres contenedores y el replica set |
| FastAPI devuelve HTTP 503 | Revisar MongoDB y `docker compose logs registro` |
| No encuentra archivos JSONL | Ejecutar primero el generador |
| Error de hashes SHA-256 | Regenerar los datos y revisar el manifiesto |
| HTTP 409 | Revisar estado, token, propietario y versión |
| HTTP 204 | No existen tareas disponibles en el proyecto |
| Cambios de Python no se aplican | Reconstruir con `docker compose up -d --build registro` |
| P8 se interrumpió | Restaurar los tres nodos y verificar `rs.status()` |
| Una prueba falla después de reiniciar | Comprobar que los servicios estén levantados |

Para revisar los logs del registro:

```bash
docker compose logs --tail=100 registro
```

Para verificar los contenedores:

```bash
docker compose ps
```

Para restaurar MongoDB después de una interrupción:

```bash
docker compose up -d mongo-1 mongo-2 mongo-3
```

### 13. Estado final del trabajo del Estudiante 1

| Apartado | Estado |
|---|---|
| Infraestructura Docker | Completado |
| Replica set MongoDB | Completado |
| Cinco colecciones y modelos | Completado |
| Cuatro índices obligatorios | Completado y evaluado |
| Generador y carga de datos | Completado |
| Servicio REST FastAPI | Implementado y probado internamente |
| Asignaciones atómicas y estados | Completado y probado internamente |
| Revisión y exportación paginada | Completado |
| Prueba P2 interna | Superada |
| Prueba P8 de MongoDB | Superada |
| Validación de contrato P6 | Ejecutada |
| Integración mediante gateway | Pendiente |
| Coordinación de locks con Valkey | Pendiente de integración |
| Entrega durable de eventos a Parquet | Pendiente de integración |

La implementación interna del subsistema de registro operacional y MongoDB está desarrollada, probada y versionada en Git.

