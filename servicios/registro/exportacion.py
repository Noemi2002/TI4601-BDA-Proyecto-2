
from math import ceil
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from pymongo.errors import PyMongoError

from database import db

router = APIRouter()


@router.get("/proyectos/{proyecto_id}/exportar")
def exportar_tareas(
    proyecto_id: UUID,
    pagina: int = Query(default=1, ge=1),
    limite: int = Query(default=50, ge=1, le=100)
):
    try:
        # Verificar que el proyecto exista
        proyecto = db.proyectos.find_one({
            "_id": str(proyecto_id)
        })

        if proyecto is None:
            raise HTTPException(
                status_code=404,
                detail="Proyecto no encontrado"
            )

        # Solamente se exportan tareas aprobadas
        filtro = {
            "proyecto_id": str(proyecto_id),
            "estado": "aprobada"
        }

        total = db.tareas.count_documents(filtro)

        desplazamiento = (pagina - 1) * limite

        cursor = (
            db.tareas.find(
                filtro,
                {
                    "_id": 1,
                    "proyecto_id": 1,
                    "batch_id": 1,
                    "modalidad": 1,
                    "geometria": 1,
                    "archivo": 1,
                    "anotacion_vigente": 1,
                    "ultima_revision": 1,
                    "version": 1
                }
            )
            .sort("_id", 1)
            .skip(desplazamiento)
            .limit(limite)
        )

        tareas = []

        for tarea in cursor:
            tareas.append({
                "tarea_id": tarea["_id"],
                "proyecto_id": tarea["proyecto_id"],
                "batch_id": tarea["batch_id"],
                "modalidad": tarea["modalidad"],
                "geometria": tarea["geometria"],
                "archivo": tarea["archivo"],
                "anotacion": tarea.get("anotacion_vigente"),
                "revision": tarea.get("ultima_revision"),
                "version": tarea["version"]
            })

        return jsonable_encoder({
            "proyecto_id": str(proyecto_id),
            "pagina": pagina,
            "limite": limite,
            "total": total,
            "total_paginas": ceil(total / limite),
            "tareas": tareas
        })

    except PyMongoError as error:
        raise HTTPException(
            status_code=503,
            detail="MongoDB no disponible para exportacion"
        ) from error
