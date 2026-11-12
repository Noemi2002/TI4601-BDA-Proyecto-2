
import os

from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

MONGO_URI = os.environ["MONGO_URI"]

# Conexion al conjunto de replicas
cliente = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,
    retryWrites=False
)

# Base de datos con escritura por mayoria
db = cliente.get_database(
    "plataforma",
    write_concern=WriteConcern(
        w="majority",
        wtimeout=5000
    )
)
