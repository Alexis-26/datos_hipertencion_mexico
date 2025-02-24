import pandas as pd

def porcentajes_nulos(datos:pd.DataFrame):
    nulos = datos.isnull() # Validar que datos son nulos
    total_nulos = nulos.sum() # Total de nulos por cada columna
    total_datos = len(datos)# Total de renglones
    porcentaje = total_nulos / total_datos # Calculando el porcentaje de datos nulos por columna
    return porcentaje

def cant_duplicados(datos:pd.DataFrame):
    duplicados = datos.duplicated() # Detectando duplicados por cada fila
    total_duplicados = duplicados.sum() # Sumando todos los duplicados
    return total_duplicados