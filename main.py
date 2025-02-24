import pandas as pd
from pymongo import MongoClient
from flask import Flask, request, jsonify
import process_data.extraction as ex
import process_data.transform as tr
from pymongo import MongoClient

# Nombre de la base de datos: hipertencio_mexico

# Realiza la conexion al cluster de MongoDB
def configuracion():
    URI = 'mongodb+srv://alexis:Chokart$2978@cluster0.dx3fa.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
    MONGODB_URI = URI
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

    # Revisar las columnas de la coleccion
    # columnas = collection.find_one()
    # print(columnas)

    # Estructura la proyeccion
    proyeccion = estructura_proyeccion(columnas)

    # Realiza la consulta en la coleccion
    resultados = list(collection.find({}, proyeccion))

    return resultados
    # Mostrar los resultados
    # for resultado in resultados:
    #     print(resultado)
    

app = Flask(__name__)

@app.route('/consulta', methods=['GET'])
def consulta():
    try:
        columnas = request.args.get('columnas')
        columnas = columnas.split(",") if columnas else []
        resultados = consulta_db(columnas)
        return jsonify(resultados)
    
    except Exception as e:
        return jsonify({"error": str(e)})
    

if __name__ == '__main__':
    app.run()