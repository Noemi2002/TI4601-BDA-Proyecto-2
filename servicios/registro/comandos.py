
import hashlib
import json
from datetime import datetime, timezone
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Body, HTTPException
from pydantic import AwareDatetime, BaseModel, Field, model_validator
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError
from pymongo.write_concern import WriteConcern

from database import cliente, db

router = APIRouter()

GEOMETRIAS = {
    "texto": {"span"},
    "imagen": {"bbox", "poligono"},
    "video": {"bbox_video"},
    "lidar": {"cuboide_3d"},
}


# Modelos de validacion

class Etiqueta(BaseModel):
    etiqueta: str = Field(min_length=1)
    instruccion: str = Field(min_length=1)


class Archivo(BaseModel):
    uri: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    bytes: int = Field(gt=0)
    mime: str = Field(min_length=1)
    meta: dict[str, Any] = Field(default_factory=dict)


class NuevaTarea(BaseModel):
    tarea_id: UUID
    tipo: str = Field(min_length=1)
    prioridad: int = Field(default=2, ge=1, le=5)
    archivo: Archivo


class CrearProyecto(BaseModel):
    accion: Literal["crear_proyecto"]
    proyecto_id: UUID
    cliente_id: UUID
    nombre: str = Field(min_length=1)
    modalidad: Literal["texto", "imagen", "video", "lidar"]
    geometria: Literal[
        "span", "bbox", "poligono", "bbox_video", "cuboide_3d"
    ]
    taxonomia: list[Etiqueta] = Field(min_length=1)
    prioridad_defecto: int = Field(default=2, ge=1, le=5)
    sla_horas: int = Field(default=48, gt=0)
    fecha_limite: AwareDatetime

    @model_validator(mode="after")
    def comprobar_geometria(self):
        if self.geometria not in GEOMETRIAS[self.modalidad]:
            raise ValueError("Geometria incompatible con modalidad")
        return self


class CrearLote(BaseModel):
    accion: Literal["crear_lote"]
    batch_id: UUID
    proyecto_id: UUID
    nombre: str = Field(min_length=1)
    descripcion: str = ""
    tareas: list[NuevaTarea] = Field(min_length=1, max_length=500)


class LiberarTarea(BaseModel):
    accion: Literal["liberar_tarea"]
    tarea_id: UUID
    anotador_id: UUID
    token: UUID


Comando = Annotated[
    CrearProyecto | CrearLote | LiberarTarea,
    Body(discriminator="accion")
]


# Crear proyecto

def crear_proyecto(c: CrearProyecto):
    if not db.clientes.find_one({"_id": str(c.cliente_id)}):
        raise HTTPException(404, "Cliente inexistente")

    # Normalizar a UTC
    fecha_limite = c.fecha_limite.astimezone(
        timezone.utc
    ).replace(tzinfo=None)

    # Ajustar a la precisión de milisegundos de MongoDB
    fecha_limite = fecha_limite.replace(
        microsecond=(fecha_limite.microsecond // 1000) * 1000
    )

    doc = {
        "_id": str(c.proyecto_id),
        "cliente_id": str(c.cliente_id),
        "nombre": c.nombre,
        "estado": "activo",
        "modalidad": c.modalidad,
        "geometria": c.geometria,
        "taxonomia": [e.model_dump() for e in c.taxonomia],
        "prioridad_defecto": c.prioridad_defecto,
        "sla_horas": c.sla_horas,
        "fecha_limite": fecha_limite,
        "creado_en": datetime.now(timezone.utc),
    }

    resultado = db.proyectos.update_one(
        {"_id": doc["_id"]},
        {"$setOnInsert": doc},
        upsert=True
    )

    existente = db.proyectos.find_one({"_id": doc["_id"]})

    comparables = (
        "cliente_id", "nombre", "estado", "modalidad",
        "geometria", "taxonomia", "prioridad_defecto",
        "sla_horas", "fecha_limite"
    )

    if any(existente.get(k) != doc[k] for k in comparables):
        raise HTTPException(
            409, "ID de proyecto usado con datos diferentes"
        )

    return {
        "proyecto_id": doc["_id"],
        "creado": resultado.upserted_id is not None
    }


# Crear lote y sus tareas

def crear_lote(c: CrearLote):
    proyecto = db.proyectos.find_one({
        "_id": str(c.proyecto_id),
        "estado": "activo"
    })

    if not proyecto:
        raise HTTPException(
            404, "Proyecto inexistente o inactivo"
        )

    ids = [str(t.tarea_id) for t in c.tareas]

    if len(set(ids)) != len(ids):
        raise HTTPException(
            422, "Hay UUID repetidos entre las tareas"
        )

    # Firma para reconocer reintentos de la misma solicitud
    payload = c.model_dump(mode="json")
    firma = hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, ensure_ascii=False
        ).encode()
    ).hexdigest()

    batch_id = str(c.batch_id)

    def confirmar_existente():
        existente = db.batches.find_one({"_id": batch_id})

        if existente is None:
            return None

        if existente.get("solicitud_hash") != firma:
            raise HTTPException(
                409, "ID de lote usado con otros datos"
            )

        total = db.tareas.count_documents({
            "batch_id": batch_id
        })

        if total != len(c.tareas):
            raise HTTPException(
                503, "Lote existente incompleto"
            )

        return {
            "batch_id": batch_id,
            "tareas": total,
            "creado": False
        }

    repetido = confirmar_existente()

    if repetido is not None:
        return repetido

    ahora = datetime.now(timezone.utc)

    lote = {
        "_id": batch_id,
        "proyecto_id": str(c.proyecto_id),
        "nombre": c.nombre,
        "descripcion": c.descripcion,
        "estado": "activo",
        "solicitud_hash": firma,
        "creado_en": ahora,
    }

    tareas = [
        {
            "_id": str(t.tarea_id),
            "proyecto_id": str(c.proyecto_id),
            "batch_id": batch_id,
            "modalidad": proyecto["modalidad"],
            "tipo": t.tipo,
            "geometria": proyecto["geometria"],
            "archivo": t.archivo.model_dump(),
            "estado": "disponible",
            "prioridad": t.prioridad,
            "asignacion": None,
            "anotacion_vigente": None,
            "ultima_revision": None,
            "ultimo_evento_id": None,
            "version": 0,
            "creado_en": ahora,
            "actualizado_en": ahora,
        }
        for t in c.tareas
    ]

    # Transaccion: lote y tareas se confirman juntos
    try:
        with cliente.start_session() as sesion:
            with sesion.start_transaction(
                write_concern=WriteConcern(
                    w="majority", wtimeout=5000
                )
            ):
                db.batches.insert_one(lote, session=sesion)
                db.tareas.insert_many(tareas, session=sesion)

    except DuplicateKeyError:
        repetido = confirmar_existente()

        if repetido is not None:
            return repetido

        raise HTTPException(
            409, "UUID de tarea o lote duplicado"
        )

    return {
        "batch_id": batch_id,
        "tareas": len(tareas),
        "creado": True
    }


# Liberar tarea

def liberar_tarea(c: LiberarTarea):
    ahora = datetime.now(timezone.utc)
    evento_id = str(uuid4())

    tarea = db.tareas.find_one_and_update(
        {
            "_id": str(c.tarea_id),
            "estado": "abierta",
            "asignacion.anotador_id": str(c.anotador_id),
            "asignacion.token": str(c.token),
        },
        {
            "$set": {
                "estado": "disponible",
                "asignacion": None,
                "ultimo_evento_id": evento_id,
                "actualizado_en": ahora,
            },
            "$inc": {"version": 1}
        },
        return_document=ReturnDocument.AFTER
    )

    if tarea is None:
        existe = db.tareas.find_one(
            {"_id": str(c.tarea_id)},
            {"_id": 1}
        )

        if existe is None:
            raise HTTPException(
                404, "Tarea no encontrada"
            )

        raise HTTPException(
            409, "Tarea no abierta o token incorrecto"
        )

    return {
        "tarea_id": tarea["_id"],
        "estado": tarea["estado"],
        "version": tarea["version"],
        "evento_id": evento_id
    }


# Sexta ruta interna

@router.post("/comandos")
def ejecutar_comando(comando: Comando):
    try:
        if isinstance(comando, CrearProyecto):
            return crear_proyecto(comando)

        if isinstance(comando, CrearLote):
            return crear_lote(comando)

        return liberar_tarea(comando)

    except PyMongoError as error:
        raise HTTPException(
            status_code=503,
            detail="MongoDB no pudo confirmar la operacion"
        ) from error
