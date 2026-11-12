# TI4601-BDA-Proyecto-2

## Configuración de MongoDB

El proyecto utiliza MongoDB 8.0.4 con un conjunto de réplicas de tres nodos denominado `rs-plataforma`.

### 1. Levantar los contenedores

Desde la raíz del repositorio, ejecutar:

```bash
docker compose up -d mongo-1 mongo-2 mongo-3
```

Verificar que los tres contenedores estén saludables:

```bash
docker compose ps
```

### 2. Inicializar el conjunto de réplicas

Este paso se realiza **una sola vez**, cuando el conjunto todavía no está inicializado.

Ingresar a MongoDB:

```bash
docker exec -it proyecto2-mongo-1 mongosh
```

Dentro de `mongosh`, ejecutar:

```javascript
rs.initiate({
  _id: "rs-plataforma",
  members: [
    { _id: 0, host: "mongo-1:27017" },
    { _id: 1, host: "mongo-2:27017" },
    { _id: 2, host: "mongo-3:27017" }
  ]
})
```

### 3. Verificar la replicación

Dentro de `mongosh`, ejecutar:

```javascript
rs.status().members.map(m => ({
  name: m.name,
  state: m.stateStr,
  health: m.health
}))
```

El resultado esperado es un nodo `PRIMARY` y dos `SECONDARY`, todos con `health: 1`.

### 4. Detener y reiniciar

Para detener los servicios conservando los datos:

```bash
docker compose stop
```

Para volver a iniciarlos:

```bash
docker compose up -d mongo-1 mongo-2 mongo-3
```

No es necesario repetir `rs.initiate()` mientras se conserven los volúmenes de MongoDB.

**Advertencia:** no utilizar `docker compose down -v`, porque elimina los volúmenes y los datos persistidos.