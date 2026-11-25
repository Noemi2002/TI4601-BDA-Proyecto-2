from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from database import cliente, db
from historial_cliente import emitir_evento
from envios import router as envios_router
from revisiones import router as revisiones_router
from exportacion import router as exportacion_router
from comandos import router as comandos_router

app = FastAPI(
    title="Registro Operacional",
    description="Servicio interno de la plataforma de anotacion",
    version="1.0.0"
)

app.include_router(envios_router)
app.include_router(revisiones_router)
app.include_router(exportacion_router)
app.include_router(comandos_router)


# Modelo de solicitud
class SolicitudAsignacion(BaseModel):
    proyecto_id: UUID
    anotador_id: UUID
    token: UUID


@app.get("/salud")
def salud():
    try:
        cliente.admin.command("ping")
        return {
            "servicio": "registro",
            "estado": "operativo",
            "mongodb": "disponible"
        }

    except PyMongoError:
        raise HTTPException(
            status_code=503,
            detail="MongoDB no disponible"
        )


@app.post("/tareas/siguiente")
def siguiente_tarea(solicitud: SolicitudAsignacion):

    proyecto_id = str(solicitud.proyecto_id)
    anotador_id = str(solicitud.anotador_id)
    token = str(solicitud.token)

    ahora = datetime.now(timezone.utc)

    try:
        # Verificar que el proyecto exista y este activo
        proyecto = db.proyectos.find_one({
            "_id": proyecto_id,
            "estado": "activo"
        })

        if proyecto is None:
            raise HTTPException(
                status_code=404,
                detail="Proyecto no encontrado o inactivo"
            )

        # Verificar que el usuario sea un anotador activo
        anotador = db.anotadores.find_one({
            "_id": anotador_id,
            "estado": "activo",
            "rol": "anotador"
        })

        if anotador is None:
            raise HTTPException(
                status_code=403,
                detail="Anotador no autorizado"
            )

        # Reservar identificador del evento
        evento_id = str(uuid4())

        # Asignacion atomica en MongoDB
        tarea = db.tareas.find_one_and_update(
            {
                "proyecto_id": proyecto_id,
                "estado": "disponible"
            },
            {
                "$set": {
                    "estado": "abierta",
                    "asignacion": {
                        "anotador_id": anotador_id,
                        "token": token,
                        "asignada_en": ahora,
                        "vence_en": ahora + timedelta(
                            seconds=120
                        )
                    },
                    "actualizado_en": ahora,
                    "ultimo_evento_id": evento_id
                },
                "$inc": {
                    "version": 1
                }
            },
            sort=[
                ("prioridad", -1),
                ("creado_en", 1),
                ("_id", 1)
            ],
            return_document=ReturnDocument.AFTER
        )

        # No quedan tareas disponibles
        if tarea is None:
            return Response(status_code=204)

        emitir_evento(
            evento_id=evento_id,
            timestamp=ahora,
            tarea=tarea,
            estado_anterior="disponible",
            estado_nuevo="abierta",
            anotador_id=anotador_id,
            toma_ts=tarea["asignacion"]["asignada_en"],
        )

        return {
            "tarea_id": tarea["_id"],
            "proyecto_id": tarea["proyecto_id"],
            "batch_id": tarea["batch_id"],
            "estado": tarea["estado"],
            "version": tarea["version"],
            "modalidad": tarea["modalidad"],
            "geometria": tarea["geometria"],
            "archivo": tarea["archivo"],
            "asignacion": {
                "anotador_id": anotador_id,
                "token": token,
                "vence_en": tarea["asignacion"][
                    "vence_en"
                ].isoformat()
            },
            "evento_id": evento_id
        }

    except PyMongoError as error:
        raise HTTPException(
            status_code=503,
            detail="MongoDB no pudo confirmar la operacion"
        ) from error