
const plataforma = db.getSiblingDB("plataforma");

// Cliente creado en la prueba anterior
const clienteId = "c9d73e54-73b2-44fc-97f1-84121df9d726";

// Verificar que el cliente exista
const cliente = plataforma.clientes.findOne({
  _id: clienteId
});

if (!cliente) {
  throw new Error("El cliente no existe");
}

// Definir el proyecto
const proyecto = {
  _id: "8c977759-642e-45c1-99b4-07f8433da75b",
  cliente_id: clienteId,
  nombre: "Anotación LiDAR - Vehículos Autónomos",
  estado: "activo",
  modalidad: "lidar",
  geometria: "cuboide_3d",

  taxonomia: [
    {
      etiqueta: "lidar#peaton",
      instruccion: "Identificar personas a pie."
    },
    {
      etiqueta: "lidar#vehiculo",
      instruccion: "Identificar vehículos."
    },
    {
      etiqueta: "lidar#ciclista",
      instruccion: "Incluir ciclistas y skaters."
    },
    {
      etiqueta: "lidar#signal",
      instruccion: "Identificar señales de tránsito."
    }
  ],

  prioridad_defecto: 2,
  sla_horas: 48,
  fecha_limite: new Date("2027-02-01T23:59:59Z"),
  creado_en: new Date()
};

// Insertar únicamente si todavía no existe
const resultado = plataforma.proyectos.updateOne(
  { _id: proyecto._id },
  { $setOnInsert: proyecto },
  {
    upsert: true,
    writeConcern: { w: "majority", wtimeout: 5000 }
  }
);

print("Resultado de la operación:");
printjson(resultado);

print("\nProyecto almacenado:");
printjson(
  plataforma.proyectos.findOne({ _id: proyecto._id })
);
