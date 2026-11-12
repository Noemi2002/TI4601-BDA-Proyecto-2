
const plataforma = db.getSiblingDB("plataforma");

// Índice 1: tareas por proyecto, estado y prioridad
plataforma.tareas.createIndex(
  { proyecto_id: 1, estado: 1, prioridad: -1 },
  { name: "idx_tareas_proyecto_estado_prioridad" }
);

// Índice 2: tareas asignadas a un anotador
plataforma.tareas.createIndex(
  { "asignacion.anotador_id": 1, estado: 1 },
  { name: "idx_tareas_anotador_estado" }
);

// Índice 3: tareas pertenecientes a un batch
plataforma.tareas.createIndex(
  { batch_id: 1 },
  { name: "idx_tareas_batch" }
);

// Índice 4: proyectos por cliente y estado
plataforma.proyectos.createIndex(
  { cliente_id: 1, estado: 1 },
  { name: "idx_proyectos_cliente_estado" }
);

// Verificar índices creados
print("\nÍndices de tareas:");
printjson(plataforma.tareas.getIndexes());

print("\nÍndices de proyectos:");
printjson(plataforma.proyectos.getIndexes());
