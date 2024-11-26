import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta

# Extraer datos de prime_readmass usando los noins de rfc_config_test

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Recuperar las credenciales de la base de datos desde el archivo .env
PGPASSWORD = os.getenv('PGPASSWORD')
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_USER = os.getenv('PG_USER')
PG_DB = os.getenv('PG_DB')

# Crear la cadena de conexión a PostgreSQL usando SQLAlchemy
DATABASE_URL = f"postgresql://{PG_USER}:{PGPASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Crear un motor de conexión
engine = create_engine(DATABASE_URL)

# Calcular la fecha del día de ayer
yesterday = (datetime.now() - timedelta(days=1)).date()

# Consulta SQL para obtener los valores de la columna medidor
medidor_query = text("""
    SELECT medidor
    FROM cgm_test.rfc_config
""")

# Ejecutar la consulta para obtener el listado de specific_noins
with engine.connect() as connection:
    medidor_df = pd.read_sql_query(medidor_query, connection)
    specific_noins = medidor_df['medidor'].tolist()  # Convertir la columna a una lista

# Consulta SQL para extraer la información
query = text("""
    SELECT *
    FROM cgm.prime_readmass
    WHERE datetime = :date AND noins = ANY(:noins)
""")

# Ejecutar la consulta y cargar los resultados en un DataFrame
with engine.connect() as connection:
    df = pd.read_sql_query(query, connection, params={"date": yesterday, "noins": specific_noins})

# Mostrar el DataFrame
print(df)


# Transformación del DataFrame
# Seleccionar y renombrar columnas según el DDL de la nueva tabla
columns_mapping = {
    "noins": "noins",
    "uom": "uom",
    "usage_data": "usage_data",
    "utcyearx": "utcyearx",
    "utcmonx": "utcmonx",
    "utcdayx": "utcdayx",
    "utchourx": "utchourx",
    "utctime_short": "utctime_short",
    "idclient": "idclient",
    "nointervals": "nointervals",
    "channel": "channel",
    "idvar": "idvar",
    "datetime": "datetime",
    "readval": "readval",
    "raw_data": "raw_data",
    "demand": "demand",
    "dst_flag": "dst_flag",
    "id_date": "id_date",
    "id_reading_detail": "id_reading_detail",
    "id_time": "id_time",
    "flag": "flag",
    "accountno": "accountno",
    "ke": "ke",
    "datex": "datex",
    "yearx": "yearx",
    "monx": "monx",
    "dayx": "dayx",
    "hourx": "hourx",
    "time_short": "time_short",
    "dow": "dow",
    "utcdow": "utcdow",
    "utcdatetime": "utcdatetime",
    "id_soc": "id_soc",
    "num_log": "num_log",
    "noins_log": "noins_log",
    "is_backup": "is_backup",
    "idsocket": "idsocket",
    "id_ori": "id_ori",
    "fecha_update": "fecha_update",
}

# Filtrar y renombrar columnas según el mapeo
transformed_df = df[list(columns_mapping.keys())].rename(columns=columns_mapping)

# Verificar la transformación
print("DataFrame transformado:")
print(transformed_df.head())

# Cargar los datos transformados a la base de datos
with engine.connect() as connection:
    # Insertar los datos en la tabla destino
    transformed_df.to_sql(
        'prime_readmass',  # Nombre de la tabla destino
        con=engine,          # Conexión a la base de datos
        schema='cgm_test',   # Esquema de la base de datos
        if_exists='append',  # Añadir registros a la tabla
        index=False,         # No incluir el índice del DataFrame
        method='multi'       # Método de inserción para mayor eficiencia
    )

print("Datos cargados correctamente en cgm_test.prime_readmass.")

# Verificar si la vista y las tablas cumplen con los requerimientos