
const plataforma = db.getSiblingDB("plataforma");

const cliente = {
  _id: "c9d73e54-73b2-44fc-97f1-84121df9d726",
  nombre: "Autonomous Driving CR",
  email: "contacto@autonomous.cr",
  creado_en: new Date()
};

// Insertar únicamente si no existe
const resultado = plataforma.clientes.updateOne(
  { _id: cliente._id },
  { $setOnInsert: cliente },
  {
    upsert: true,
    writeConcern: { w: "majority", wtimeout: 5000 }
  }
);

print("Resultado de la operación:");
printjson(resultado);

print("\nCliente almacenado:");
printjson(plataforma.clientes.findOne({ _id: cliente._id }));
