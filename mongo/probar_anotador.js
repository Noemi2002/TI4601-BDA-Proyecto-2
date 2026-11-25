
const plataforma = db.getSiblingDB("plataforma");

// Identificador compartido por los miembros del equipo
const equipoId = "e71a2b43-64f7-4c9e-a992-8314e8211b24";

// Personas de prueba
const anotadores = [
  {
    _id: "a10d4130-9a67-4cc6-a6f2-7a397c1021a0",
    nombre: "Ana Rodríguez",
    email: "ana@plataforma.com",
    equipo_id: equipoId,
    rol: "anotador",
    estado: "activo",
    creado_en: new Date()
  },
  {
    _id: "b21d5241-8b78-4dd7-b7f3-8b408d2132b1",
    nombre: "Carlos Méndez",
    email: "carlos@plataforma.com",
    equipo_id: equipoId,
    rol: "revisor",
    estado: "activo",
    creado_en: new Date()
  }
];

// Insertar personas sin duplicarlas
for (const anotador of anotadores) {
  const resultado = plataforma.anotadores.updateOne(
    { _id: anotador._id },
    { $setOnInsert: anotador },
    {
      upsert: true,
      writeConcern: { w: "majority", wtimeout: 5000 }
    }
  );

  print(`Procesado: ${anotador.nombre}`);
  printjson(resultado);
}

// Consultar los miembros del equipo
print("\nMiembros del equipo:");

plataforma.anotadores.find({
  equipo_id: equipoId
}).forEach(printjson);
