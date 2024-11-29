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

# Crear la cadena de conexión
DATABASE_URL = f"postgresql://{PG_USER}:{PGPASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Crear el motor de conexión (puedes descomentar si lo usas después)
engine = create_engine(DATABASE_URL)

# Directorio de los archivos
data_dir = "datos_termonorte"

# Listar todos los archivos en el directorio
all_files = os.listdir(data_dir)

# Clasificar los archivos por tipo basado en los nombres (como COMB140, oefagnd, etc.)
file_types = {}
for file in all_files:
    if file.endswith(('.txa', '.tx2', '.txf')):  # Filtrar extensiones válidas
        name_part = ''.join([c for c in file if not c.isdigit()])  # Extraer prefijo antes de los números
        base_name = name_part.split('.')[0]
        if base_name not in file_types:
            file_types[base_name] = []
        file_types[base_name].append(file)

# Función para extraer fecha del nombre del archivo
def extract_date_from_filename(file_name):
    # Ejemplo de nombre: COMB140101.txa
    base_name = file_name.split('.')[0]
    # Extraer los últimos cuatro caracteres (mes y día)
    date_part = ''.join(filter(str.isdigit, base_name))[-4:]
    month = date_part[:2]
    day = date_part[2:]
    # Asumir año 2024
    return datetime.strptime(f"2024-{month}-{day}", "%Y-%m-%d")

# Leer cada tipo de archivo en un DataFrame
dataframes = {}
for file_type, files in file_types.items():
    df_list = []
    for file in files:
        file_path = os.path.join(data_dir, file)
        try:
            # Leer archivo con la primera fila como encabezado
            df = pd.read_csv(file_path, sep=";", header=0, encoding="latin-1")  # Cambia encoding si es necesario
            # Extraer fecha del nombre del archivo
            fecha_operacion = extract_date_from_filename(file)
            # Extraer versión (extensión del archivo)
            version = file.split('.')[-1]
            # Agregar columna fechaoperacion y versión
            df['fechaoperacion'] = fecha_operacion
            df['version'] = version
            df_list.append(df)
        except Exception as e:
            print(f"Error al leer el archivo {file_path}: {e}")
    # Combinar todos los DataFrames de este tipo en uno solo
    if df_list:
        dataframes[file_type] = pd.concat(df_list, ignore_index=True)

# Transformar el DataFrame para `tdia_sis` si existe
if "tdia_sis" in dataframes:
    df_tdia_sis = dataframes["tdia_sis"]
    
    # Separar año, mes y día desde la columna `fechaoperacion`
    df_tdia_sis["tdsisano"] = df_tdia_sis["fechaoperacion"].dt.year
    df_tdia_sis["tdsismes"] = df_tdia_sis["fechaoperacion"].dt.month
    df_tdia_sis["tdsisdia"] = df_tdia_sis["fechaoperacion"].dt.day
    
    # Renombrar columnas según la tabla en la imagen
    df_tdia_sis = df_tdia_sis.rename(columns={
        "version": "tdsisver",  # Tipo (versión)
        df_tdia_sis.columns[0]: "tdsiscod",  # Código
        df_tdia_sis.columns[1]: "tdsisdes",  # Descripción
        df_tdia_sis.columns[2]: "tdsisval",  # Valor
        "fechaoperacion": "tdsisfecha"      # Fecha de operación
    })

    # Asegurarse de que `tdsistipo` esté en mayúsculas
    df_tdia_sis["tdsisver"] = df_tdia_sis["tdsisver"].str.upper()

    # Reordenar columnas según el orden de la tabla en la imagen
    df_tdia_sis = df_tdia_sis[
        ["tdsisano", "tdsismes", "tdsisdia", "tdsisver", 
         "tdsiscod", "tdsisdes", "tdsisval", "tdsisfecha"]
    ]

    # Actualizar el DataFrame modificado en el diccionario
    dataframes["tdia_sis"] = df_tdia_sis
    
    try:
        # Cargar el DataFrame a la tabla tmng.tdia_sis
        df_tdia_sis.to_sql("tdia_sis", engine, schema="tmng", if_exists="replace", index=False)
        print("Datos cargados exitosamente en tmng.tdia_sis")
    except Exception as e:
        print(f"Error al cargar los datos en tmng.tdia_sis: {e}")
        
# Transformar el DataFrame para `totaldia` si existe
if "totaldia" in dataframes:
    df_totaldia = dataframes["totaldia"]
    
    # Renombrar columnas según la tabla en la imagen
    df_totaldia = df_totaldia.rename(columns={
        df_totaldia.columns[0]: "codigo",        # Código
        df_totaldia.columns[1]: "descripcion",  # Descripción
        df_totaldia.columns[2]: "planta",       # Planta
        df_totaldia.columns[3]: "valor",        # Valor
        "fechaoperacion": "fechaoperacion",     # Fecha de operación
        "version": "version"                    # Versión
    })

    # Agregar una columna updated_at con la fecha y hora actual
    df_totaldia["updated_at"] = pd.Timestamp.now()

    # Reordenar columnas según el orden de la tabla en la imagen
    df_totaldia = df_totaldia[
        ["codigo", "descripcion", "planta", "valor", 
         "fechaoperacion", "version", "updated_at"]
    ]

    # Cargar el DataFrame a la tabla tmng.totaldia
    try:
        df_totaldia.to_sql("totaldia", engine, schema="tmng", if_exists="replace", index=False)
        print("Datos cargados exitosamente en tmng.totaldia")
    except Exception as e:
        print(f"Error al cargar los datos en tmng.totaldia: {e}")
else:
    print("El DataFrame totaldia no está disponible para la carga.")

# Mostrar información de los DataFrames creados
for file_type, df in dataframes.items():
    print(f"\nDataFrame para tipo {file_type}:\n", df.head())
