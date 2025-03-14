import pandas as pd
from pymongo import MongoClient
from flask import Flask, request, jsonify
from bson.decimal128 import Decimal128 # Importa Decimal128
import json # Importa json

# Nombre de la base de datos: hipertencio_mexico

# Realiza la conexion al cluster de MongoDB
def configuracion():
    MONGODB_URI = 'mongodb+srv://alexis:Chokart$2978@cluster0.dx3fa.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
    client = MongoClient(MONGODB_URI)
    return client

# Estructura la proyeccion de la consulta segun las columnas recibidas
def estructura_proyeccion(columnas):
    proyeccion = {"_id": 0}
    if columnas:
        for columna in columnas:
            proyeccion[columna] = 1
    return proyeccion

# Realiza la consulta en la coleccion
def consulta_db(columnas):
    # Realiza la conexion
    client = configuracion()

    # Selecciona la base de datos
    db = client["hipertencio_mexico"]

    # Selecciona la coleccion (tabla)
    collection = db["registro"]

    # Estructura la proyeccion
    proyeccion = estructura_proyeccion(columnas)

    # Realiza la consulta en la coleccion
    resultados = list(collection.find({}, proyeccion))

    return resultados

# Función para serializar Decimal128 a string
def decimal_default(obj):
    if isinstance(obj, Decimal128):
        return str(obj)
    raise TypeError(repr(obj) + " is not JSON serializable")

app = Flask(__name__)

@app.route('/consulta', methods=['GET'])
def consulta():
    try:
        columnas = request.args.get('columnas')
        columnas = columnas.split(",") if columnas else []
        resultados = consulta_db(columnas)
        # Serializa los resultados a JSON usando la función decimal_default
        json_resultados = json.dumps(resultados, default=decimal_default)
        return json_resultados, 200, {'Content-Type': 'application/json'} # Asegura el tipo de contenido
    
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)