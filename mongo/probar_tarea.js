
const plataforma = db.getSiblingDB("plataforma");

// Identificadores creados en las pruebas anteriores
const proyectoId = "8c977759-642e-45c1-99b4-07f8433da75b";
const batchId = "9a620fb4-1756-4ae8-a712-29ab9ca5bf5a";

// Verificar que exista el proyecto
const proyecto = plataforma.proyectos.findOne({
  _id: proyectoId
});

if (!proyecto) {
  throw new Error("El proyecto no existe");
}

// Verificar que exista el batch
const batch = plataforma.batches.findOne({
  _id: batchId
});

if (!batch) {
  throw new Error("El batch no existe");
}

// Comprobar que el batch pertenezca al proyecto
if (batch.proyecto_id !== proyectoId) {
  throw new Error("El batch no pertenece al proyecto");
}

// Crear una tarea de prueba
const tarea = {
  _id: "81e5853e-f739-4c7b-88b9-98f6965e679c",

  proyecto_id: proyectoId,
  batch_id: batchId,

  modalidad: "lidar",
  tipo: "deteccion_3d",
  geometria: "cuboide_3d",

  archivo: {
    uri: "s3://datos-ejemplo/lidar/escena-001.bin",
    sha256: "a".repeat(64),
    bytes: 5242880,
    mime: "application/octet-stream",
    meta: {
      puntos: 120000,
      distancia_min: 0,
      distancia_max: 100
    }
  },

  estado: "disponible",
  prioridad: 2,

  asignacion: null,
  anotacion_vigente: null,
  ultima_revision: null,
  ultimo_evento_id: null,

  version: 0,

  creado_en: new Date(),
  actualizado_en: new Date()
};

// Insertar si todavía no existe
const resultado = plataforma.tareas.updateOne(
  { _id: tarea._id },
  { $setOnInsert: tarea },
  {
    upsert: true,
    writeConcern: {
      w: "majority",
      wtimeout: 5000
    }
  }
);

print("Resultado de la operación:");
printjson(resultado);

print("\nTarea almacenada:");
printjson(
  plataforma.tareas.findOne({ _id: tarea._id })
);
