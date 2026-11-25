import os
import uuid
from typing import Any
import httpx
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel



REGISTRO_URL = os.getenv(
    "REGISTRO_URL",
    "http://registro:8001",
)

SESION_URL = os.getenv(
    "SESION_URL",
    "http://sesion:8002",
)

TIMEOUT = 5.0

HISTORIAL_URL = os.getenv("HISTORIAL_URL", "http://historial:8003")
ANALITICA_URL = os.getenv("ANALITICA_URL", "http://analitica:8004")
# La primera consulta arranca Spark (decenas de segundos), por eso el timeout es largo.
TIMEOUT_ANALITICA = float(os.getenv("ANALITICA_TIMEOUT", "300"))

app = FastAPI(
    title="Gateway Plataforma de Anotación",
    version="1.0.0",
)


# =========================================================
# MODELOS PÚBLICOS
# =========================================================

class SolicitudSiguiente(BaseModel):
    proyecto_id: uuid.UUID
    anotador_id: uuid.UUID


class SolicitudBorrador(BaseModel):
    proyecto_id: uuid.UUID
    anotador_id: uuid.UUID
    token: uuid.UUID
    datos: dict[str, Any]

class SolicitudLiberar(BaseModel):
    proyecto_id: uuid.UUID
    anotador_id: uuid.UUID
    token: uuid.UUID

class SolicitudRecuperarSesion(BaseModel):
    anotador_id: uuid.UUID

class SolicitudEnviar(BaseModel):
    proyecto_id: uuid.UUID
    anotador_id: uuid.UUID
    token: uuid.UUID
    anotacion: dict[str, Any]

class SolicitudRevision(BaseModel):
    revisor_id: uuid.UUID
    accion: str
    codigo_motivo: str | None = None

class EtiquetaProyecto(BaseModel):
    etiqueta: str
    instruccion: str


class SolicitudCrearProyecto(BaseModel):
    proyecto_id: uuid.UUID
    cliente_id: uuid.UUID
    nombre: str
    modalidad: str
    geometria: str
    taxonomia: list[EtiquetaProyecto]
    prioridad_defecto: int = 2
    sla_horas: int = 48
    fecha_limite: str


class ArchivoTarea(BaseModel):
    uri: str
    sha256: str
    bytes: int
    mime: str
    meta: dict[str, Any] = {}


class NuevaTareaGateway(BaseModel):
    tarea_id: uuid.UUID
    tipo: str
    prioridad: int = 2
    archivo: ArchivoTarea


class SolicitudCrearLote(BaseModel):
    batch_id: uuid.UUID
    nombre: str
    descripcion: str = ""
    tareas: list[NuevaTareaGateway]

@app.post("/proyectos")
async def crear_proyecto(
    body: SolicitudCrearProyecto,
):
    payload = {
        "accion": "crear_proyecto",
        "proyecto_id": str(body.proyecto_id),
        "cliente_id": str(body.cliente_id),
        "nombre": body.nombre,
        "modalidad": body.modalidad,
        "geometria": body.geometria,
        "taxonomia": [
            item.model_dump()
            for item in body.taxonomia
        ],
        "prioridad_defecto": body.prioridad_defecto,
        "sla_horas": body.sla_horas,
        "fecha_limite": body.fecha_limite,
    }

    async with httpx.AsyncClient() as cliente:
        try:
            respuesta = await cliente.post(
                f"{REGISTRO_URL}/comandos",
                json=payload,
                timeout=TIMEOUT,
            )
        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de registro no disponible",
            )

        try:
            contenido = respuesta.json()
        except ValueError:
            contenido = {"detail": respuesta.text}

        return JSONResponse(
            status_code=respuesta.status_code,
            content=contenido,
        )


@app.post("/proyectos/{proyecto_id}/lotes")
async def crear_lote(
    proyecto_id: uuid.UUID,
    body: SolicitudCrearLote,
):
    payload = {
        "accion": "crear_lote",
        "batch_id": str(body.batch_id),
        "proyecto_id": str(proyecto_id),
        "nombre": body.nombre,
        "descripcion": body.descripcion,
        "tareas": [
            {
                **tarea.model_dump(
                    exclude={"tarea_id"}
                ),
                "tarea_id": str(tarea.tarea_id),
            }
            for tarea in body.tareas
        ],
    }

    async with httpx.AsyncClient() as cliente:
        try:
            respuesta = await cliente.post(
                f"{REGISTRO_URL}/comandos",
                json=payload,
                timeout=TIMEOUT,
            )
        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de registro no disponible",
            )

        try:
            contenido = respuesta.json()
        except ValueError:
            contenido = {"detail": respuesta.text}

        return JSONResponse(
            status_code=respuesta.status_code,
            content=contenido,
        )

@app.get("/proyectos/{proyecto_id}/exportar")
async def exportar_proyecto(
    proyecto_id: uuid.UUID,
    pagina: int = 1,
    limite: int = 50,
):
    if pagina < 1:
        raise HTTPException(
            status_code=422,
            detail="pagina debe ser >= 1",
        )

    if limite < 1 or limite > 100:
        raise HTTPException(
            status_code=422,
            detail="limite debe estar entre 1 y 100",
        )

    async with httpx.AsyncClient() as cliente:
        try:
            respuesta = await cliente.get(
                f"{REGISTRO_URL}/proyectos/"
                f"{proyecto_id}/exportar",
                params={
                    "pagina": pagina,
                    "limite": limite,
                },
                timeout=TIMEOUT,
            )
        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de registro no disponible",
            )

        try:
            contenido = respuesta.json()
        except ValueError:
            contenido = {"detail": respuesta.text}

        return JSONResponse(
            status_code=respuesta.status_code,
            content=contenido,
        )




@app.post("/tareas/{tarea_id}/enviar")
async def enviar_tarea(
    tarea_id: uuid.UUID,
    body: SolicitudEnviar,
):
    tarea_id_str = str(tarea_id)
    proyecto_id = str(body.proyecto_id)
    anotador_id = str(body.anotador_id)
    token = str(body.token)

    async with httpx.AsyncClient() as cliente:

        # 1. Comprobar ownership en Valkey.
        try:
            respuesta_lock = await cliente.post(
                f"{SESION_URL}/sesiones/recuperar",
                json={
                    "tarea_id": tarea_id_str,
                    "token": token,
                },
                timeout=TIMEOUT,
            )
        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de sesión no disponible",
            )

        if respuesta_lock.status_code != 200:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "No se pudo validar el lock de la tarea"
                },
            )

        lock = respuesta_lock.json().get("lock", {})

        if (
            not lock.get("activo")
            or not lock.get("token_coincide")
        ):
            return JSONResponse(
                status_code=409,
                content={
                    "detail": (
                        "El token no corresponde o "
                        "el lock ya venció"
                    )
                },
            )

        # 2. Confirmar la anotación en MongoDB.
        try:
            respuesta_registro = await cliente.post(
                f"{REGISTRO_URL}/tareas/{tarea_id_str}/enviar",
                json={
                    "anotador_id": anotador_id,
                    "token": token,
                    "anotacion": body.anotacion,
                },
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:
            # Muy importante:
            # NO borramos el borrador.
            raise HTTPException(
                status_code=503,
                detail=(
                    "MongoDB no disponible; "
                    "el borrador permanece en Valkey"
                ),
            )

        if respuesta_registro.status_code != 200:
            try:
                detalle = respuesta_registro.json()
            except ValueError:
                detalle = {
                    "detail": respuesta_registro.text
                }

            return JSONResponse(
                status_code=respuesta_registro.status_code,
                content=detalle,
            )

        resultado = respuesta_registro.json()

        # 3. Mongo confirmó: ahora sí limpiamos
        # sesión, lock y borrador.
        try:
            await cliente.request(
                "DELETE",
                f"{SESION_URL}/locks/liberar",
                json={
                    "tarea_id": tarea_id_str,
                    "proyecto_id": proyecto_id,
                    "anotador_id": anotador_id,
                    "token": token,
                    "eliminar_borrador": True,
                },
                timeout=TIMEOUT,
            )
        except httpx.HTTPError:
            # Mongo ya confirmó el envío.
            # No podemos deshacer esa transición.
            pass

        return resultado

@app.put("/tareas/{tarea_id}/borrador")
async def guardar_borrador(
    tarea_id: uuid.UUID,
    body: SolicitudBorrador,
):
    tarea_id_str = str(tarea_id)
    proyecto_id = str(body.proyecto_id)
    anotador_id = str(body.anotador_id)
    token = str(body.token)

    async with httpx.AsyncClient() as cliente:

        # -------------------------------------------------
        # 1. RENOVAR ASIGNACIÓN DEFINITIVA EN MONGODB
        # -------------------------------------------------

        try:
            respuesta_registro = await cliente.post(
                f"{REGISTRO_URL}/comandos",
                json={
                    "accion": "renovar_asignacion",
                    "tarea_id": tarea_id_str,
                    "anotador_id": anotador_id,
                    "token": token,
                },
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de registro no disponible",
            )

        if respuesta_registro.status_code != 200:
            try:
                detalle = respuesta_registro.json()
            except ValueError:
                detalle = {
                    "detail": respuesta_registro.text
                }

            return JSONResponse(
                status_code=respuesta_registro.status_code,
                content=detalle,
            )

        # -------------------------------------------------
        # 2. GUARDAR BORRADOR Y RENOVAR LOCK EN VALKEY
        # -------------------------------------------------

        try:
            respuesta_sesion = await cliente.put(
                f"{SESION_URL}/borradores/{tarea_id_str}",
                json={
                    "proyecto_id": proyecto_id,
                    "anotador_id": anotador_id,
                    "token": token,
                    "datos": body.datos,
                },
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:

            compensada = await liberar_asignacion(
                cliente,
                tarea_id_str,
                anotador_id,
                token,
            )

            raise HTTPException(
                status_code=503,
                detail={
                    "mensaje": (
                        "Servicio de sesión no disponible"
                    ),
                    "asignacion_compensada": compensada,
                },
            )

        if respuesta_sesion.status_code != 200:

            compensada = await liberar_asignacion(
                cliente,
                tarea_id_str,
                anotador_id,
                token,
            )

            try:
                detalle = respuesta_sesion.json()
            except ValueError:
                detalle = {
                    "detail": respuesta_sesion.text
                }

            return JSONResponse(
                status_code=respuesta_sesion.status_code,
                content={
                    "detail": detalle,
                    "asignacion_compensada": compensada,
                },
            )

        return {
            "tarea_id": tarea_id_str,
            "estado": "borrador_guardado",
            "lock": respuesta_sesion.json(),
        }


@app.post("/tareas/{tarea_id}/revision")
async def revisar_tarea(
    tarea_id: uuid.UUID,
    body: SolicitudRevision,
):
    tarea_id_str = str(tarea_id)

    if body.accion not in {
        "iniciar",
        "aprobar",
        "rechazar",
    }:
        raise HTTPException(
            status_code=422,
            detail="Acción de revisión inválida",
        )

    if (
        body.accion == "rechazar"
        and not body.codigo_motivo
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                "codigo_motivo es obligatorio "
                "al rechazar"
            ),
        )

    payload = {
        "revisor_id": str(body.revisor_id),
        "accion": body.accion,
    }

    if body.codigo_motivo is not None:
        payload["codigo_motivo"] = body.codigo_motivo

    async with httpx.AsyncClient() as cliente:
        try:
            respuesta = await cliente.post(
                f"{REGISTRO_URL}/tareas/"
                f"{tarea_id_str}/revision",
                json=payload,
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de registro no disponible",
            )

        try:
            contenido = respuesta.json()
        except ValueError:
            contenido = {
                "detail": respuesta.text,
            }

        return JSONResponse(
            status_code=respuesta.status_code,
            content=contenido,
        )

@app.post("/tareas/{tarea_id}/liberar")
async def liberar_tarea_publica(
    tarea_id: uuid.UUID,
    body: SolicitudLiberar,
):
    tarea_id_str = str(tarea_id)
    proyecto_id = str(body.proyecto_id)
    anotador_id = str(body.anotador_id)
    token = str(body.token)

    async with httpx.AsyncClient() as cliente:

        # 1. MongoDB es la fuente de verdad:
        #    primero liberamos la asignación definitiva.
        try:
            respuesta_registro = await cliente.post(
                f"{REGISTRO_URL}/comandos",
                json={
                    "accion": "liberar_tarea",
                    "tarea_id": tarea_id_str,
                    "anotador_id": anotador_id,
                    "token": token,
                },
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de registro no disponible",
            )

        if respuesta_registro.status_code != 200:
            try:
                detalle = respuesta_registro.json()
            except ValueError:
                detalle = {
                    "detail": respuesta_registro.text
                }

            return JSONResponse(
                status_code=respuesta_registro.status_code,
                content=detalle,
            )

        # 2. Limpiar lock, sesión y borrador temporal.
        try:
            respuesta_sesion = await cliente.request(
                "DELETE",
                f"{SESION_URL}/locks/liberar",
                json={
                    "tarea_id": tarea_id_str,
                    "proyecto_id": proyecto_id,
                    "anotador_id": anotador_id,
                    "token": token,
                    "eliminar_borrador": True,
                },
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:
            return JSONResponse(
                status_code=503,
                content={
                    "detail": (
                        "La tarea fue liberada en Registro, "
                        "pero no se pudo limpiar la sesión"
                    ),
                    "registro_liberado": True,
                },
            )

        if respuesta_sesion.status_code != 200:
            try:
                detalle_sesion = respuesta_sesion.json()
            except ValueError:
                detalle_sesion = {
                    "detail": respuesta_sesion.text
                }

            return JSONResponse(
                status_code=503,
                content={
                    "detail": detalle_sesion,
                    "registro_liberado": True,
                },
            )

        return {
            "tarea_id": tarea_id_str,
            "estado": "disponible",
            "liberada": True,
        }


@app.post("/sesiones/recuperar")
async def recuperar_sesion_publica(
    body: SolicitudRecuperarSesion,
):
    anotador_id = str(body.anotador_id)

    async with httpx.AsyncClient() as cliente:

        try:
            respuesta = await cliente.post(
                f"{SESION_URL}/sesiones/recuperar",
                json={
                    "anotador_id": anotador_id,
                },
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de sesión no disponible",
            )

        if respuesta.status_code != 200:
            try:
                detalle = respuesta.json()
            except ValueError:
                detalle = {
                    "detail": respuesta.text,
                }

            return JSONResponse(
                status_code=respuesta.status_code,
                content=detalle,
            )

        recuperacion = respuesta.json()

        return {
            "anotador_id": anotador_id,
            **recuperacion,
        }

@app.get("/analitica/{consulta}")
async def analitica(
    consulta: str,
    conjunto: str = "principal",
    ref: str | None = None,
    limite: int = 1000,
):
    params = {"conjunto": conjunto, "limite": limite}
    if ref is not None:
        params["ref"] = ref

    async with httpx.AsyncClient() as cliente:
        try:
            respuesta = await cliente.get(
                f"{ANALITICA_URL}/consultas/{consulta}",
                params=params,
                timeout=TIMEOUT_ANALITICA,
            )
        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de analítica no disponible",
            )

        try:
            contenido = respuesta.json()
        except ValueError:
            contenido = {"detail": respuesta.text}

        return JSONResponse(
            status_code=respuesta.status_code,
            content=contenido,
        )

# =========================================================
# AUXILIARES
# =========================================================

async def consultar_salud(
    cliente: httpx.AsyncClient,
    nombre: str,
    url: str,
):
    try:
        respuesta = await cliente.get(
            f"{url}/salud",
            timeout=2.0,
        )

        if respuesta.status_code == 200:
            return {
                "estado": "disponible",
                "detalle": respuesta.json(),
            }

        return {
            "estado": "no_disponible",
            "http_status": respuesta.status_code,
        }

    except httpx.HTTPError:
        return {
            "estado": "no_disponible",
        }


async def liberar_asignacion(
    cliente: httpx.AsyncClient,
    tarea_id: str,
    anotador_id: str,
    token: str,
) -> bool:
    """
    Operación compensatoria.

    Se usa cuando MongoDB ya asignó una tarea,
    pero no fue posible crear el lock/sesión.
    """

    payload = {
        "accion": "liberar_tarea",
        "tarea_id": tarea_id,
        "anotador_id": anotador_id,
        "token": token,
    }

    try:
        respuesta = await cliente.post(
            f"{REGISTRO_URL}/comandos",
            json=payload,
            timeout=TIMEOUT,
        )

        return respuesta.status_code == 200

    except httpx.HTTPError:
        return False

async def recuperar_vencidas_sin_lock(
    cliente: httpx.AsyncClient,
    proyecto_id: str,
):
    """
    Recupera en MongoDB únicamente asignaciones vencidas
    cuyo lock ya no existe en Valkey.

    Si Sesión/Valkey no puede comprobarse, no se recupera
    la tarea: se aplica un comportamiento conservador.
    """

    try:
        respuesta = await cliente.post(
            f"{REGISTRO_URL}/comandos",
            json={
                "accion": "consultar_vencidas",
                "proyecto_id": proyecto_id,
                "limite": 100,
            },
            timeout=TIMEOUT,
        )
    except httpx.HTTPError:
        return

    if respuesta.status_code != 200:
        return

    candidatas = respuesta.json().get(
        "candidatas",
        [],
    )

    for candidata in candidatas:
        tarea_id = candidata.get("tarea_id")
        token = candidata.get("token")
        version = candidata.get("version")

        if not tarea_id or not token or version is None:
            continue

        # Primero comprobamos Valkey mediante el
        # servicio de Sesión.
        try:
            respuesta_lock = await cliente.post(
                f"{SESION_URL}/sesiones/recuperar",
                json={
                    "tarea_id": tarea_id,
                    "token": token,
                },
                timeout=TIMEOUT,
            )
        except httpx.HTTPError:
            # No podemos demostrar que el lock haya
            # desaparecido. No recuperamos.
            continue

        if respuesta_lock.status_code != 200:
            continue

        lock = respuesta_lock.json().get(
            "lock",
            {},
        )

        # Si todavía existe un lock, la tarea no se toca.
        if lock.get("activo"):
            continue

        # El lock ya no existe: ahora sí se permite
        # recuperar la asignación vencida en MongoDB.
        try:
            await cliente.post(
                f"{REGISTRO_URL}/comandos",
                json={
                    "accion": "recuperar_vencida",
                    "tarea_id": tarea_id,
                    "token": token,
                    "version": version,
                },
                timeout=TIMEOUT,
            )
        except httpx.HTTPError:
            # Otra solicitud podría haberla recuperado
            # concurrentemente. No hacemos retry.
            continue

# =========================================================
# SALUD
# =========================================================

@app.get("/salud")
async def salud():
    async with httpx.AsyncClient() as cliente:

        registro = await consultar_salud(
            cliente,
            "registro",
            REGISTRO_URL,
        )

        sesion = await consultar_salud(
            cliente,
            "sesion",
            SESION_URL,
        )
        
        historial = await consultar_salud(cliente, "historial", HISTORIAL_URL)
        analitica_capa = await consultar_salud(cliente, "analitica", ANALITICA_URL)

    disponibles = sum(
        capa["estado"] == "disponible"
        for capa in [registro, sesion]
    )

    if disponibles == 2:
        estado = "operativo"

    elif disponibles == 0:
        estado = "caido"

    else:
        estado = "degradado"

    return {
        "servicio": "gateway",
        "estado": estado,
        "capas": {
                "capas_analiticas": {
                    "historial": historial,
                    "analitica": analitica_capa,
        },
            "registro": registro,
            "sesion": sesion,
        },
    }


# =========================================================
# SIGUIENTE TAREA
# =========================================================

@app.post("/tareas/siguiente")
async def siguiente_tarea(body: SolicitudSiguiente):

    proyecto_id = str(body.proyecto_id)
    anotador_id = str(body.anotador_id)

    # El Gateway genera el token.
    token = str(uuid.uuid4())

    solicitud_registro = {
        "proyecto_id": proyecto_id,
        "anotador_id": anotador_id,
        "token": token,
    }

    async with httpx.AsyncClient() as cliente:
        # Antes de buscar una nueva tarea, devolvemos
        # a la cola las asignaciones vencidas cuyo
        # lock de Valkey ya desapareció.
        await recuperar_vencidas_sin_lock(
            cliente,
            proyecto_id,
        )

        # -------------------------------------------------
        # 1. ADQUIRIR LA TAREA EN MONGODB
        # -------------------------------------------------

        try:
            respuesta_registro = await cliente.post(
                f"{REGISTRO_URL}/tareas/siguiente",
                json=solicitud_registro,
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:
            raise HTTPException(
                status_code=503,
                detail="Servicio de registro no disponible",
            )

        # No hay tareas.
        if respuesta_registro.status_code == 204:
            return Response(status_code=204)

        # Propagamos errores producidos por Registro.
        if respuesta_registro.status_code != 200:
            try:
                detalle = respuesta_registro.json()
            except ValueError:
                detalle = {
                    "detail": respuesta_registro.text
                }

            return JSONResponse(
                status_code=respuesta_registro.status_code,
                content=detalle,
            )

        tarea = respuesta_registro.json()

        tarea_id = tarea.get("tarea_id")

        if not tarea_id:
            raise HTTPException(
                status_code=502,
                detail=(
                    "Registro respondió sin tarea_id"
                ),
            )

        # -------------------------------------------------
        # 2. CREAR LOCK Y SESIÓN
        # -------------------------------------------------

        solicitud_sesion = {
            "tarea_id": tarea_id,
            "proyecto_id": proyecto_id,
            "anotador_id": anotador_id,
            "token": token,
            "etapa": "editando",
        }

        try:
            respuesta_sesion = await cliente.post(
                f"{SESION_URL}/locks/adquirir",
                json=solicitud_sesion,
                timeout=TIMEOUT,
            )

        except httpx.HTTPError:

            compensada = await liberar_asignacion(
                cliente,
                tarea_id,
                anotador_id,
                token,
            )

            raise HTTPException(
                status_code=503,
                detail={
                    "mensaje": (
                        "Servicio de sesión no disponible"
                    ),
                    "asignacion_compensada": compensada,
                },
            )

        # -------------------------------------------------
        # 3. SI VALKEY RECHAZÓ EL LOCK
        #    COMPENSAMOS MONGODB
        # -------------------------------------------------

        if respuesta_sesion.status_code != 200:

            compensada = await liberar_asignacion(
                cliente,
                tarea_id,
                anotador_id,
                token,
            )

            try:
                detalle_sesion = respuesta_sesion.json()
            except ValueError:
                detalle_sesion = {
                    "detail": respuesta_sesion.text
                }

            status = (
                respuesta_sesion.status_code
                if respuesta_sesion.status_code == 409
                else 503
            )

            return JSONResponse(
                status_code=status,
                content={
                    "detail": detalle_sesion,
                    "asignacion_compensada": compensada,
                },
            )

        # -------------------------------------------------
        # 4. BUSCAR BORRADOR RECUPERABLE
        # -------------------------------------------------

        borrador = None

        try:
            respuesta_recuperacion = await cliente.post(
                f"{SESION_URL}/sesiones/recuperar",
                json={
                    "anotador_id": anotador_id,
                },
                timeout=TIMEOUT,
            )

            if respuesta_recuperacion.status_code == 200:
                recuperacion = (
                    respuesta_recuperacion.json()
                )

                posible_borrador = recuperacion.get(
                    "borrador"
                )

                # Solo consideramos recuperable el
                # borrador del mismo anotador.
                if (
                    posible_borrador
                    and posible_borrador.get(
                        "anotador_id"
                    ) == anotador_id
                ):
                    borrador = posible_borrador

        except httpx.HTTPError:
            # La tarea y su lock ya quedaron creados.
            # No hacemos fallar toda la asignación solo
            # porque no haya podido recuperarse borrador.
            pass

        # -------------------------------------------------
        # 5. RESPUESTA PÚBLICA
        # -------------------------------------------------

        return {
            **tarea,
            "token": token,
            "borrador": borrador,
        }