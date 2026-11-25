from datetime import datetime, timezone
from typing import Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from database import db
from historial_cliente import emitir_evento

router = APIRouter()


class SolicitudRevision(BaseModel):
    revisor_id: UUID
    accion: Literal["iniciar", "aprobar", "rechazar"]
    codigo_motivo: str | None = Field(
        default=None,
        max_length=100
    )


@router.post("/tareas/{tarea_id}/revision")
def revisar_tarea(
    tarea_id: UUID,
    solicitud: SolicitudRevision
):
    ahora = datetime.now(timezone.utc)
    evento_id = str(uuid4())

    try:
        # 1. Verificar que la persona sea revisora
        revisor = db.anotadores.find_one({
            "_id": str(solicitud.revisor_id),
            "rol": "revisor",
            "estado": "activo"
        })

        if revisor is None:
            raise HTTPException(
                status_code=403,
                detail="Revisor no autorizado"
            )

        # 2. Validar que el rechazo tenga motivo
        if solicitud.accion == "rechazar":
            if not solicitud.codigo_motivo or not solicitud.codigo_motivo.strip():
                raise HTTPException(
                    status_code=422,
                    detail="El rechazo requiere codigo_motivo"
                )

        # 3. Preparar las condiciones atomicas
        filtro = {"_id": str(tarea_id)}

        cambios = {
            "actualizado_en": ahora,
            "ultimo_evento_id": evento_id
        }

        if solicitud.accion == "iniciar":
            filtro["estado"] = "anotada"

            cambios.update({
                "estado": "en_revision",
                "revision_en_curso": {
                    "revisor_id": str(solicitud.revisor_id),
                    "iniciada_en": ahora
                }
            })

            estado_evento = "en_revision"

        else:
            # Solo quien inicio la revision puede cerrarla
            filtro["estado"] = "en_revision"
            filtro["revision_en_curso.revisor_id"] = str(
                solicitud.revisor_id
            )

            decision = (
                "aprobada"
                if solicitud.accion == "aprobar"
                else "rechazada"
            )

            cambios["ultima_revision"] = {
                "revisor_id": str(solicitud.revisor_id),
                "decision": decision,
                "codigo_motivo": (
                    solicitud.codigo_motivo.strip()
                    if decision == "rechazada"
                    else None
                ),
                "revisada_en": ahora
            }

            cambios["revision_en_curso"] = None

            if solicitud.accion == "aprobar":
                cambios["estado"] = "aprobada"
                estado_evento = "aprobada"

            else:
                # En MongoDB el rechazo devuelve a disponible
                cambios["estado"] = "disponible"
                cambios["asignacion"] = None
                estado_evento = "rechazada"

        # 4. Cambiar el estado atomica y condicionalmente
        tarea = db.tareas.find_one_and_update(
            filtro,
            {
                "$set": cambios,
                "$inc": {"version": 1}
            },
            return_document=ReturnDocument.AFTER
        )

        # 5. Diferenciar tarea inexistente y conflicto
        if tarea is None:
            existe = db.tareas.find_one(
                {"_id": str(tarea_id)},
                {"_id": 1}
            )

            if existe is None:
                raise HTTPException(
                    status_code=404,
                    detail="Tarea no encontrada"
                )

            raise HTTPException(
                status_code=409,
                detail="Transicion invalida o revisor incorrecto"
            )

        asig = tarea.get("asignacion") or {}
        vigente = tarea.get("anotacion_vigente") or {}
        emitir_evento(
            evento_id=evento_id,
            timestamp=ahora,
            tarea=tarea,
            estado_anterior=(
                "anotada" if solicitud.accion == "iniciar"
                else "en_revision"
            ),
            estado_nuevo=estado_evento,
            # En el rechazo asignacion queda en None: se toma de anotacion_vigente
            anotador_id=asig.get("anotador_id") or vigente.get("anotador_id"),
            revisor_id=solicitud.revisor_id,
            codigo_motivo=(
                (tarea.get("ultima_revision") or {}).get("codigo_motivo")
                if estado_evento == "rechazada" else None
            ),
            toma_ts=(
                asig.get("asignada_en")
                if estado_evento == "aprobada" else None
            ),
        )

        return {
            "tarea_id": tarea["_id"],
            "estado": tarea["estado"],
            "estado_evento": estado_evento,
            "version": tarea["version"],
            "evento_id": evento_id,
            "ultima_revision": tarea.get("ultima_revision")
        }

    except PyMongoError as error:
        raise HTTPException(
            status_code=503,
            detail="MongoDB no pudo confirmar la revision"
        ) from error