import os
import subprocess
from dotenv import load_dotenv

class PostgresBackupRestore:
    def __init__(self, env_path=".env", table_name=None, source_schema="public", target_schema="public_test"):
        # Cargar las variables de entorno desde el archivo .env
        load_dotenv(env_path)
        
        # Asignar las variables de entorno a atributos de la clase
        self.pg_password = os.getenv("PGPASSWORD")
        self.pg_host = os.getenv("PG_HOST")
        self.pg_port = os.getenv("PG_PORT")
        self.pg_user = os.getenv("PG_USER")
        self.pg_db = os.getenv("PG_DB")
        self.source_schema = source_schema
        self.target_schema = target_schema
        
        self.table_name=table_name 
    
    def backup_table(self):
        """
        Ejecuta el comando de backup de la tabla especificada.
        """
        print("Iniciando el backup de la tabla...")
        backup_command = (
            f"docker run --rm "
            f"-e PGPASSWORD='{self.pg_password}' "
            f"postgres:15.8 "
            f"pg_dump "
            f"-h {self.pg_host} "
            f"-p {self.pg_port} "
            f"-U {self.pg_user} "
            f"-d {self.pg_db} "
            f"-t {self.source_schema}.{self.table_name} "
            f"> {self.table_name}_backup.backup"
        )
        
        try:
            subprocess.run(backup_command, shell=True, check=True)
            print(f"Backup completado exitosamente: {self.table_name}_backup.backup")
        except subprocess.CalledProcessError as e:
            print(f"Error al realizar el backup: {e}")
    
    def replace_schema(self):
        """
        Ejecuta el comando sed para reemplazar el esquema en el archivo de backup.
        """
        print("Reemplazando el esquema en el archivo de backup...")
        sed_command = (
            f"sed 's/{self.source_schema}.{self.table_name}/{self.target_schema}.{self.table_name}/g' "
            f"{self.table_name}_backup.backup > {self.table_name}_restore.sql"
        )
        
        try:
            subprocess.run(sed_command, shell=True, check=True)
            print(f"Esquema reemplazado y archivo generado: {self.table_name}_restore.sql")
        except subprocess.CalledProcessError as e:
            print(f"Error al reemplazar el esquema: {e}")
    
    def restore_table(self):
        """
        Ejecuta el comando de restauración de la tabla en el esquema de prueba.
        """
        print("Iniciando la restauración de la tabla en el esquema de prueba...")
        restore_command = (
            f"docker run --rm "
            f"-v ./{self.table_name}_restore.sql:/{self.table_name}_restore.sql "
            f"-e PGPASSWORD='{self.pg_password}' "
            f"postgres:15.8 "
            f"psql "
            f"-h {self.pg_host} "
            f"-p {self.pg_port} "
            f"-U {self.pg_user} "
            f"-d {self.pg_db} "
            f"-f {self.table_name}_restore.sql"
        )
        
        try:
            subprocess.run(restore_command, shell=True, check=True)
            print("Restauración completada exitosamente.")
        except subprocess.CalledProcessError as e:
            print(f"Error al restaurar la tabla: {e}")
            
    def cleanup_files(self):
        """
        Elimina los archivos de backup y restore generados.
        """
        print("Iniciando la limpieza de archivos generados...")
        backup_file = f"{self.table_name}_backup.backup"
        restore_file = f"{self.table_name}_restore.sql"
        
        for file_path in [backup_file, restore_file]:
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"Archivo eliminado exitosamente: {file_path}")
                else:
                    print(f"El archivo no existe y no puede ser eliminado: {file_path}")
            except Exception as e:
                print(f"Error al eliminar el archivo {file_path}: {e}")


def validar_input(mensaje):
    while True:
        valor = input(mensaje)
        if valor.isalpha():
            return valor
        else:
            print("Por favor, ingrese solo texto (sin números ni caracteres especiales).")

if __name__ == "__main__":
    table_name = validar_input("Ingrese la tabla de origen: ")
    source_schema = validar_input("Ingrese el esquema de origen: ")
    target_schema = validar_input("Ingrese el esquema de destino: ")
    
    backup_restore = PostgresBackupRestore(table_name=table_name, 
                                           source_schema=source_schema,
                                           target_schema=target_schema)
    try:
        backup_restore.backup_table()
        backup_restore.replace_schema()
        backup_restore.restore_table()
    except Exception as e:
        print(f"Ocurrió un error durante el proceso: {e}")
    finally:
        backup_restore.cleanup_files()
        print('Finalizado')