from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de credenciales de la base de datos
PGPASSWORD = os.getenv('PGPASSWORD')
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_USER = os.getenv('PG_USER')
PG_DB = os.getenv('PG_DB')

# Verificar que todas las variables de entorno estén cargadas
if not all([PGPASSWORD, PG_HOST, PG_PORT, PG_USER, PG_DB]):
    raise ValueError("Faltan variables de entorno necesarias para la conexión a la base de datos.")

# Crear la cadena de conexión
DATABASE_URL = f"postgresql://{PG_USER}:{PGPASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Crear el motor de conexión
engine = create_engine(DATABASE_URL)

# Ruta al archivo CSV
csv_file_path = 'vep.csv'

# Leer el archivo CSV con pandas
try:
    df = pd.read_csv(
        csv_file_path,
        sep=';',                # Separador de campos
        encoding='latin-1',      # Codificación del archivo
        dtype={'FECHA': str, 'FUENTE': str, 'VALOR (KPCD)': str}  # Definir tipos de datos iniciales
    )
    df.columns = ['FECHA', 'FUENTE', 'VALOR (KPCD)']
    print("Archivo CSV cargado exitosamente.")
except FileNotFoundError:
    raise FileNotFoundError(f"No se encontró el archivo {csv_file_path}.")
except Exception as e:
    raise Exception(f"Error al leer el archivo CSV: {e}")

# Mostrar las primeras filas para verificar la carga
print("Primeras filas del DataFrame cargado:")
print(df.head())

# Limpiar y transformar los datos

# Convertir la columna 'FECHA' a formato datetime
try:
    df['FECHA'] = pd.to_datetime(df['FECHA'], format='%d/%m/%Y')
except Exception as e:
    raise ValueError(f"Error al convertir la columna 'FECHA' a datetime: {e}")

# Reemplazar comas en 'VALOR (KPCD)' y convertir a float
try:
    df['VALOR (KPCD)'] = df['VALOR (KPCD)'].str.replace(',', '').astype(float)
except Exception as e:
    raise ValueError(f"Error al convertir la columna 'VALOR (KPCD)' a float: {e}")

# Renombrar columnas para que coincidan con la tabla de la base de datos
df.rename(columns={
    'FECHA': 'fechaoperacion',
    'FUENTE': 'fuente',
    'VALOR (KPCD)': 'valor_kpc'
}, inplace=True)

# Verificar la estructura del DataFrame después de las transformaciones
print("Estructura del DataFrame después de la limpieza:")
print(df.info())

# Insertar los datos en la tabla de PostgreSQL
try:
    df.to_sql(
        'volumen_entregado_productor',  # Nombre de la tabla
        schema='public',                 # Esquema de la tabla
        con=engine,                      # Motor de conexión
        if_exists='append',              # Acción si la tabla ya existe
        index=False                      # No insertar el índice del DataFrame
    )
    print("Datos insertados exitosamente en la tabla 'volumen_entregado_productor'.")
except Exception as e:
    raise Exception(f"Error al insertar los datos en la base de datos: {e}")
