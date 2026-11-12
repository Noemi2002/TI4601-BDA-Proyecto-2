
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from database import db

router = APIRouter()


class Anotacion(BaseModel):
    geometria: Literal[
        "span", "bbox", "poligono",
        "bbox_video", "cuboide_3d"
    ]
    etiquetas: list[str] = Field(min_length=1)
    elementos: list[dict[str, Any]] = Field(
        min_length=1
    )


class SolicitudEnvio(BaseModel):
    anotador_id: UUID
    token: UUID
    anotacion: Anotacion


@router.post("/tareas/{tarea_id}/enviar")
def enviar_tarea(
    tarea_id: UUID,
    solicitud: SolicitudEnvio
):
    try:
        # Comprobar que la tarea existe
        tarea_actual = db.tareas.find_one({
            "_id": str(tarea_id)
        })

        if tarea_actual is None:
            raise HTTPException(
                status_code=404,
                detail="Tarea no encontrada"
            )

        # Validar la geometria
        if (
            solicitud.anotacion.geometria
            != tarea_actual["geometria"]
        ):
            raise HTTPException(
                status_code=422,
                detail="Geometria incompatible"
            )

        # Comprobar las etiquetas del proyecto
        proyecto = db.proyectos.find_one({
            "_id": tarea_actual["proyecto_id"]
        })

        if proyecto is None:
            raise HTTPException(
                status_code=409,
                detail="Proyecto de la tarea no existe"
            )

        permitidas = {
            item["etiqueta"]
            for item in proyecto["taxonomia"]
        }

        if not set(
            solicitud.anotacion.etiquetas
        ).issubset(permitidas):
            raise HTTPException(
                status_code=422,
                detail="Etiquetas no permitidas"
            )

        ahora = datetime.now(timezone.utc)
        evento_id = str(uuid4())

        # Actualizacion atomica
        tarea = db.tareas.find_one_and_update(
            {
                "_id": str(tarea_id),
                "estado": "abierta",
                "asignacion.anotador_id": str(
                    solicitud.anotador_id
                ),
                "asignacion.token": str(
                    solicitud.token
                ),
                "asignacion.vence_en": {
                    "$gt": ahora
                }   
            },
            {
                "$set": {
                    "estado": "anotada",
                    "anotacion_vigente": {
                        **solicitud.anotacion.model_dump(),
                        "anotador_id": str(
                            solicitud.anotador_id
                        ),
                        "enviada_en": ahora
                    },
                    "ultimo_evento_id": evento_id,
                    "actualizado_en": ahora
                },
                "$inc": {
                    "version": 1
                }
            },
            return_document=ReturnDocument.AFTER
        )

        if tarea is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Tarea no abierta o token "
                    "de asignacion incorrecto"
                )
            )

        return {
            "tarea_id": tarea["_id"],
            "estado": tarea["estado"],
            "version": tarea["version"],
            "evento_id": evento_id
        }

    except PyMongoError as error:
        raise HTTPException(
            status_code=503,
            detail="MongoDB no pudo confirmar el envio"
        ) from error
