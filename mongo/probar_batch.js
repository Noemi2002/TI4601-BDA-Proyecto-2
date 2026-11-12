
const plataforma = db.getSiblingDB("plataforma");

// Proyecto creado anteriormente
const proyectoId = "8c977759-642e-45c1-99b4-07f8433da75b";

// Verificar que el proyecto exista
const proyecto = plataforma.proyectos.findOne({
  _id: proyectoId
});

if (!proyecto) {
  throw new Error("El proyecto no existe");
}

if (proyecto.estado !== "activo") {
  throw new Error("El proyecto no está activo");
}

// Definir el lote
const batch = {
  _id: "9a620fb4-1756-4ae8-a712-29ab9ca5bf5a",
  proyecto_id: proyectoId,
  nombre: "Lote LiDAR 001",
  descripcion: "Primer conjunto de nubes de puntos LiDAR",
  estado: "activo",
  creado_en: new Date()
};

// Insertar solamente si no existe
const resultado = plataforma.batches.updateOne(
  { _id: batch._id },
  { $setOnInsert: batch },
  {
    upsert: true,
    writeConcern: { w: "majority", wtimeout: 5000 }
  }
);

print("Resultado de la operación:");
printjson(resultado);

print("\nBatch almacenado:");
printjson(
  plataforma.batches.findOne({ _id: batch._id })
);
