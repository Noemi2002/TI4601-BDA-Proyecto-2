import json
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from redis.exceptions import RedisError

from valkey_client import LOCK_TTL_MS, valkey


app = FastAPI(
    title="Servicio de Sesión",
    version="1.0.0",
)


# =========================================================
# MODELOS
# =========================================================

class AdquirirLockRequest(BaseModel):
    tarea_id: str
    proyecto_id: str
    anotador_id: str
    token: str
    etapa: str = "editando"


class RenovarLockRequest(BaseModel):
    tarea_id: str
    proyecto_id: str
    anotador_id: str
    token: str


class LiberarLockRequest(BaseModel):
    tarea_id: str
    proyecto_id: str
    anotador_id: str
    token: str
    eliminar_borrador: bool = False


class BorradorRequest(BaseModel):
    proyecto_id: str
    anotador_id: str
    token: str
    datos: dict[str, Any]


class RecuperarSesionRequest(BaseModel):
    anotador_id: str | None = None
    tarea_id: str | None = None
    token: str | None = None


# =========================================================
# UTILIDADES
# =========================================================

def ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ahora_ms() -> int:
    return int(time.time() * 1000)


def lock_key(tarea_id: str) -> str:
    return f"lock:tarea:{tarea_id}"


def sesion_key(anotador_id: str) -> str:
    return f"sesion:{anotador_id}"


def borrador_key(tarea_id: str) -> str:
    return f"borrador:{tarea_id}"


def editando_key(proyecto_id: str) -> str:
    return f"editando:{proyecto_id}"


# =========================================================
# LUA: RENOVACIÓN SEGURA
# =========================================================

RENOVAR_LOCK_SCRIPT = """
local actual = redis.call("GET", KEYS[1])

if not actual then
    return -1
end

if actual ~= ARGV[1] then
    return -2
end

redis.call("PEXPIRE", KEYS[1], ARGV[2])

redis.call(
    "HSET",
    KEYS[2],
    "tarea_id", ARGV[3],
    "proyecto_id", ARGV[4],
    "anotador_id", ARGV[5],
    "token", ARGV[1],
    "etapa", "editando",
    "ultimo_latido", ARGV[6]
)

redis.call(
    "ZADD",
    KEYS[3],
    ARGV[7],
    ARGV[5]
)

return redis.call("PTTL", KEYS[1])
"""


# =========================================================
# LUA: GUARDAR BORRADOR + RENOVAR LOCK
# =========================================================

GUARDAR_BORRADOR_SCRIPT = """
local actual = redis.call("GET", KEYS[1])

if not actual then
    return -1
end

if actual ~= ARGV[1] then
    return -2
end

redis.call("PEXPIRE", KEYS[1], ARGV[2])

redis.call(
    "HSET",
    KEYS[2],
    "tarea_id", ARGV[3],
    "proyecto_id", ARGV[4],
    "anotador_id", ARGV[5],
    "token", ARGV[1],
    "etapa", "editando",
    "ultimo_latido", ARGV[6]
)

redis.call(
    "HSET",
    KEYS[3],
    "tarea_id", ARGV[3],
    "proyecto_id", ARGV[4],
    "anotador_id", ARGV[5],
    "token", ARGV[1],
    "datos", ARGV[7],
    "actualizado_en", ARGV[6]
)

redis.call(
    "ZADD",
    KEYS[4],
    ARGV[8],
    ARGV[5]
)

return redis.call("PTTL", KEYS[1])
"""


# =========================================================
# LUA: LIBERACIÓN SEGURA
# =========================================================

LIBERAR_LOCK_SCRIPT = """
local actual = redis.call("GET", KEYS[1])

if not actual then
    return -1
end

if actual ~= ARGV[1] then
    return -2
end

redis.call("DEL", KEYS[1])

local token_sesion = redis.call("HGET", KEYS[2], "token")

if token_sesion == ARGV[1] then
    redis.call("DEL", KEYS[2])
end

redis.call(
    "ZREM",
    KEYS[3],
    ARGV[2]
)

if ARGV[3] == "1" then
    redis.call("DEL", KEYS[4])
end

return 1
"""


# =========================================================
# 1. SALUD
# =========================================================

@app.get("/salud")
def salud():
    try:
        disponible = valkey.ping()

        appendonly = valkey.config_get("appendonly").get(
            "appendonly"
        )

        appendfsync = valkey.config_get("appendfsync").get(
            "appendfsync"
        )

        return {
            "servicio": "sesion",
            "estado": (
                "operativo"
                if disponible
                else "caido"
            ),
            "valkey": (
                "disponible"
                if disponible
                else "no_disponible"
            ),
            "persistencia": {
                "appendonly": appendonly,
                "appendfsync": appendfsync,
            },
        }

    except RedisError:
        raise HTTPException(
            status_code=503,
            detail="Valkey no disponible",
        )


# =========================================================
# 2. ADQUIRIR LOCK
# =========================================================

@app.post("/locks/adquirir")
def adquirir_lock(body: AdquirirLockRequest):
    llave_lock = lock_key(body.tarea_id)

    try:
        adquirido = valkey.set(
            llave_lock,
            body.token,
            nx=True,
            px=LOCK_TTL_MS,
        )

        if not adquirido:
            raise HTTPException(
                status_code=409,
                detail="La tarea ya posee un lock activo",
            )

        try:
            with valkey.pipeline(
                transaction=True
            ) as pipe:

                pipe.hset(
                    sesion_key(body.anotador_id),
                    mapping={
                        "tarea_id": body.tarea_id,
                        "proyecto_id": body.proyecto_id,
                        "anotador_id": body.anotador_id,
                        "token": body.token,
                        "etapa": body.etapa,
                        "ultimo_latido": ahora_iso(),
                    },
                )

                pipe.zadd(
                    editando_key(body.proyecto_id),
                    {
                        body.anotador_id: ahora_ms()
                    },
                )

                pipe.execute()

        except RedisError:
            actual = valkey.get(llave_lock)

            if actual == body.token:
                valkey.delete(llave_lock)

            raise

        return {
            "adquirido": True,
            "tarea_id": body.tarea_id,
            "anotador_id": body.anotador_id,
            "ttl_ms": valkey.pttl(llave_lock),
        }

    except HTTPException:
        raise

    except RedisError:
        raise HTTPException(
            status_code=503,
            detail="Valkey no disponible",
        )


# =========================================================
# 3. RENOVAR LOCK / LATIDO
# =========================================================

@app.put("/locks/renovar")
def renovar_lock(body: RenovarLockRequest):
    instante = ahora_iso()

    try:
        resultado = valkey.eval(
            RENOVAR_LOCK_SCRIPT,
            3,

            lock_key(body.tarea_id),
            sesion_key(body.anotador_id),
            editando_key(body.proyecto_id),

            body.token,
            LOCK_TTL_MS,
            body.tarea_id,
            body.proyecto_id,
            body.anotador_id,
            instante,
            ahora_ms(),
        )

        if resultado == -1:
            raise HTTPException(
                status_code=409,
                detail="El lock ya venció",
            )

        if resultado == -2:
            raise HTTPException(
                status_code=409,
                detail="El token no corresponde al lock",
            )

        return {
            "renovado": True,
            "tarea_id": body.tarea_id,
            "ttl_ms": resultado,
        }

    except HTTPException:
        raise

    except RedisError:
        raise HTTPException(
            status_code=503,
            detail="Valkey no disponible",
        )


# =========================================================
# 4. LIBERAR LOCK
# =========================================================

@app.delete("/locks/liberar")
def liberar_lock(body: LiberarLockRequest):
    try:
        resultado = valkey.eval(
            LIBERAR_LOCK_SCRIPT,
            4,

            lock_key(body.tarea_id),
            sesion_key(body.anotador_id),
            editando_key(body.proyecto_id),
            borrador_key(body.tarea_id),

            body.token,
            body.anotador_id,
            (
                "1"
                if body.eliminar_borrador
                else "0"
            ),
        )

        if resultado == -1:
            raise HTTPException(
                status_code=409,
                detail="El lock ya no existe",
            )

        if resultado == -2:
            raise HTTPException(
                status_code=409,
                detail="El token no corresponde al lock",
            )

        return {
            "liberado": True,
            "tarea_id": body.tarea_id,
        }

    except HTTPException:
        raise

    except RedisError:
        raise HTTPException(
            status_code=503,
            detail="Valkey no disponible",
        )


# =========================================================
# 5. BORRADOR
# =========================================================

@app.put("/borradores/{tarea_id}")
def guardar_borrador(
    tarea_id: str,
    body: BorradorRequest,
):
    instante = ahora_iso()

    datos_json = json.dumps(
        body.datos,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    try:
        resultado = valkey.eval(
            GUARDAR_BORRADOR_SCRIPT,
            4,

            lock_key(tarea_id),
            sesion_key(body.anotador_id),
            borrador_key(tarea_id),
            editando_key(body.proyecto_id),

            body.token,
            LOCK_TTL_MS,
            tarea_id,
            body.proyecto_id,
            body.anotador_id,
            instante,
            datos_json,
            ahora_ms(),
        )

        if resultado == -1:
            raise HTTPException(
                status_code=409,
                detail="El lock ya venció",
            )

        if resultado == -2:
            raise HTTPException(
                status_code=409,
                detail="El token no corresponde al lock",
            )

        return {
            "guardado": True,
            "tarea_id": tarea_id,
            "actualizado_en": instante,
            "ttl_ms": resultado,
        }

    except HTTPException:
        raise

    except RedisError:
        raise HTTPException(
            status_code=503,
            detail="Valkey no disponible",
        )


# =========================================================
# 6. RECUPERAR SESIÓN
# =========================================================

@app.post("/sesiones/recuperar")
def recuperar_sesion(body: RecuperarSesionRequest):
    # -------------------------------------------------
    # MODO 1:
    # consultar el estado de un lock concreto
    # -------------------------------------------------
    if body.tarea_id is not None:
        if body.token is None:
            raise HTTPException(
                status_code=422,
                detail="token es obligatorio al consultar un lock",
            )

        try:
            clave = lock_key(body.tarea_id)

            token_actual = valkey.get(clave)
            ttl_ms = valkey.pttl(clave)

            return {
                "tarea_id": body.tarea_id,
                "lock": {
                    "activo": token_actual is not None,
                    "token_coincide": (
                        token_actual == body.token
                        if token_actual is not None
                        else False
                    ),
                    "ttl_ms": ttl_ms,
                },
            }

        except redis.RedisError:
            raise HTTPException(
                status_code=503,
                detail="Valkey no disponible",
            )

    # -------------------------------------------------
    # MODO 2:
    # recuperar sesión de un anotador
    # -------------------------------------------------
    if body.anotador_id is None:
        raise HTTPException(
            status_code=422,
            detail="anotador_id es obligatorio",
        )

    try:
        sesion = valkey.hgetall(
            sesion_key(body.anotador_id)
        )

        if not sesion:
            raise HTTPException(
                status_code=404,
                detail="No existe una sesión recuperable",
            )

        tarea_id = sesion.get("tarea_id")

        borrador = None

        if tarea_id:
            datos_borrador = valkey.hgetall(
                borrador_key(tarea_id)
            )

            if datos_borrador:
                borrador = dict(datos_borrador)

                datos_json = borrador.get("datos")

                if datos_json:
                    try:
                        borrador["datos"] = json.loads(
                            datos_json
                        )
                    except json.JSONDecodeError:
                        pass

        token_actual = None
        ttl_ms = -2

        if tarea_id:
            token_actual = valkey.get(
                lock_key(tarea_id)
            )
            ttl_ms = valkey.pttl(
                lock_key(tarea_id)
            )

        return {
            "sesion": sesion,
            "borrador": borrador,
            "lock": {
                "activo": token_actual is not None,
                "token_coincide": (
                    token_actual == sesion.get("token")
                    if token_actual is not None
                    else False
                ),
                "ttl_ms": ttl_ms,
            },
        }

    except HTTPException:
        raise

    except redis.RedisError:
        raise HTTPException(
            status_code=503,
            detail="Valkey no disponible",
        )