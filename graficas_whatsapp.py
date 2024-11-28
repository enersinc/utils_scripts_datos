import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Recuperar las credenciales de la base de datos desde el archivo .env
PGPASSWORD = os.getenv('ETRM_PGPASSWORD')
PG_HOST = os.getenv('ETRM_PG_HOST')
PG_PORT = os.getenv('ETRM_PG_PORT')
PG_USER = os.getenv('ETRM_PG_USER')
PG_DB = os.getenv('ETRM_PG_DB')

# Crear la cadena de conexión a PostgreSQL usando SQLAlchemy
DATABASE_URL = f"postgresql://{PG_USER}:{PGPASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Crea el motor de SQLAlchemy
engine = create_engine(DATABASE_URL)

# Crea una sesión
Session = sessionmaker(bind=engine)
session = Session()

def obtener_clientes_por_concepto(concepto):
    """
    Obtiene la lista de clientes de la tabla `app.maestra_whatsapp` 
    que cumplen con el filtro de concepto y activo.

    :param concepto: El valor del concepto a buscar (string).
    :return: Lista de clientes que cumplen con el filtro.
    """
    try:
        query = text("""
            SELECT cliente 
            FROM app.maestra_whatsapp 
            WHERE concepto = :concepto 
            AND activo ILIKE '%true%'
        """)
        # Ejecuta la consulta con el parámetro
        resultados = session.execute(query, {"concepto": concepto}).fetchall()
        
        # Extrae los clientes de los resultados
        clientes = [row[0] for row in resultados]
        return clientes
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return []
    finally:
        session.close()

import requests

def ejecutar_endpoints(concepto, fecha, token, clientes):
    """
    Ejecuta dos endpoints secuencialmente con los parámetros proporcionados.

    :param concepto: El concepto a utilizar (string).
    :param fecha: La fecha para los endpoints (string en formato YYYY-MM-DD).
    :param token: El Bearer Token para la autenticación (string).
    :param clientes: Listado de clientes que tienen el concepto en producción
    :return: Respuestas de los dos endpoints.
    """
    # Headers para incluir el Bearer Token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # URL del primer endpoint
    url1 = f"https://reportes.enersinc.com/{concepto}/{fecha}/whatsapp"
    try:
        # Realizar la solicitud al primer endpoint
        response1 = requests.get(url1, headers=headers)
        response1.raise_for_status()  # Lanza una excepción si ocurre un error
    except requests.exceptions.RequestException as e:
        print(f"Error al ejecutar el primer endpoint: {e}")
        return None

    # URL base del segundo endpoint
    url2_base = f"https://reportes.enersinc.com/envio/{concepto}"

    respuestas = []
    for cliente in clientes:
        url2 = f"{url2_base}/{cliente}/{fecha}"
        try:
            # Realizar la solicitud al segundo endpoint
            response2 = requests.get(url2, headers=headers)
            response2.raise_for_status()  # Lanza una excepción si ocurre un error
            respuestas.append({cliente: 'enviado'})
        except requests.exceptions.RequestException as e:
            print(f"Error al ejecutar el segundo endpoint para cliente '{cliente}': {e}")
            respuestas.append({cliente: None})

    return respuestas

# Ejemplo de uso
if __name__ == "__main__":
    concepto = "5"  # Reemplaza con el concepto deseado
    fecha = "2024-11-28"  # Reemplaza con la fecha deseada
    token = """********"""  # Reemplaza con tu Bearer Token real
    
    clientes = obtener_clientes_por_concepto(concepto)
    print("Clientes encontrados:", clientes)
    resultados = ejecutar_endpoints(concepto, fecha, token, clientes)
    print("Resultados finales:", resultados)
