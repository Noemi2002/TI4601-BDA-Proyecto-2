
const plataforma = db.getSiblingDB("plataforma");

const colecciones = [
  "clientes",
  "proyectos",
  "batches",
  "tareas",
  "anotadores"
];

for (const nombre of colecciones) {
  if (!plataforma.getCollectionNames().includes(nombre)) {
    plataforma.createCollection(nombre, {
      writeConcern: { w: "majority" }
    });

    print(`Colección creada: ${nombre}`);
  } else {
    print(`La colección ${nombre} ya existe`);
  }
}

print("\nColecciones de plataforma:");
printjson(plataforma.getCollectionNames());
