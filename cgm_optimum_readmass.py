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
    FROM cgm.optimum_readmass
""")

# Ejecutar la consulta y cargar los resultados en un DataFrame
with engine.connect() as connection:
    df = pd.read_sql_query(query, connection, params={"noins": specific_noins})

# Mostrar el DataFrame
print(df)


# Transformación del DataFrame
# Seleccionar y renombrar columnas según el DDL de la nueva tabla
columns_mapping = {"m_profile_id":"m_profile_id",
"meter_id":"meter_id",
"meter_t0":"meter_t0",
"meter_tf":"meter_tf",
"channel":"channel",
"channel_unit":"channel_unit",
"val":"val",
"raw_unit":"raw_unit",
"val_demand":"val_demand",
"val_edit":"val_edit",
"ke":"ke",
"datetime_pc":"datetime_pc",
#"idsocket":"idsocket",
#"is_backup":"is_backup",
#"fecha_update":"fecha_update"
}


# Filtrar y renombrar columnas según el mapeo
transformed_df = df[list(columns_mapping.keys())].rename(columns=columns_mapping)
transformed_df['idsocket'] = 'prueba'
transformed_df['is_backup'] = 0

# Verificar la transformación
print("DataFrame transformado:")
print(transformed_df.head())

# Cargar los datos transformados a la base de datos
with engine.connect() as connection:
    # Insertar los datos en la tabla destino
    transformed_df.to_sql(  
        'optimum_readmass',  # Nombre de la tabla destino
        con=engine,          # Conexión a la base de datos
        schema='cgm_test',   # Esquema de la base de datos
        if_exists='append',  # Añadir registros a la tabla
        index=False,         # No incluir el índice del DataFrame
        method='multi'       # Método de inserción para mayor eficiencia
    )

print("Datos cargados correctamente en cgm_test.optimum_readmass.")

# Verificar si la vista y las tablas cumplen con los requerimientos