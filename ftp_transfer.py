import re
import os
import paramiko
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def get_env_var(var_name):
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"La variable de entorno {var_name} no está definida.")
    return value

def connect_sftp(host, user, password):
    print(f"Conectando a {host} por SFTP...")
    transport = paramiko.Transport((host, 22))
    transport.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    # Guardamos el transporte para cerrarlo luego
    sftp.custom_transport = transport 
    print(f"Conectado exitosamente a {host}.")
    return sftp

def delete_old_files(sftp, file_pattern, current_date):
    """
    Elimina archivos en el directorio actual del SFTP que coincidan con el patrón
    y cuya fecha sea anterior a la fecha actual.
    """
    print(f"Buscando archivos antiguos para eliminar con patrón: {file_pattern}...")
    
    # Convertir patrón strftime a regex
    # Escapamos puntos y reemplazamos los formatos de fecha por grupos de captura
    regex_pattern = re.escape(file_pattern)
    # Handle both escaped and unescaped % just in case
    regex_pattern = regex_pattern.replace(r'%d', r'(?P<day>\d{2})').replace(r'\%d', r'(?P<day>\d{2})')
    regex_pattern = regex_pattern.replace(r'%m', r'(?P<month>\d{2})').replace(r'\%m', r'(?P<month>\d{2})')
    regex_pattern = regex_pattern.replace(r'%Y', r'(?P<year>\d{4})').replace(r'\%Y', r'(?P<year>\d{4})')
    # Aseguramos que coincida con todo el nombre
    regex_pattern = f"^{regex_pattern}$"
    
    try:
        files = sftp.listdir()
    except Exception as e:
        print(f"Error listando archivos: {e}")
        files = []

    deleted_count = 0
    for filename in files:
        match = re.match(regex_pattern, filename)
        if match:
            try:
                data = match.groupdict()
                file_date = datetime(int(data['year']), int(data['month']), int(data['day']))
                
                # Comparamos solo fechas (sin hora)
                if file_date.date() < current_date.date():
                    print(f"Eliminando archivo antiguo: {filename} (Fecha: {file_date.date()})")
                    try:
                        sftp.remove(filename)
                        deleted_count += 1
                        print(f"Eliminado correctamente: {filename}")
                    except Exception as e:
                        print(f"Error eliminando {filename}: {e}")
                else:
                    # Es el archivo de hoy o futuro, lo mantenemos
                    # print(f"Conservando archivo: {filename} (Fecha: {file_date.date()})")
                    pass
            except ValueError as e:
                print(f"Error procesando fecha de {filename}: {e}")
                continue

    if deleted_count == 0:
        print("No se encontraron archivos antiguos para eliminar.")
    else:
        print(f"Total archivos eliminados: {deleted_count}")

def main():
    try:
        # Configuración
        source_host = get_env_var("SOURCE_HOST")
        source_user = get_env_var("SOURCE_USER")
        source_pass = get_env_var("SOURCE_PASS")
        source_dir_backup = get_env_var("SOURCE_DIR_BACKUP")
        source_dir_trafico = get_env_var("SOURCE_DIR_TRAFICO")
        
        dest_dir_backup = get_env_var("DEST_DIR_BACKUP")
        dest_dir_trafico = get_env_var("DEST_DIR_TRAFICO")

        file_pattern_1 = get_env_var("FILE_PATTERN_1")
        file_pattern_2 = get_env_var("FILE_PATTERN_2")

        # Generar nombres de archivos con la fecha actual
        now = datetime.now() 
        file1 = now.strftime(file_pattern_1)
        file2 = now.strftime(file_pattern_2) 
        files_to_transfer = [file1,file2]

        print(f"Archivos a descargar (fecha actual): {files_to_transfer}")

        # 1. Descargar de Origen
        sftp_source = connect_sftp(source_host, source_user, source_pass)
        
        # Definir lista de transferencias: (nombre_archivo, directorio_origen, directorio_destino, patron_borrado)
        # file1 = Trafico -> source_dir_trafico -> dest_dir_trafico -> file_pattern_2
        # file2 = Backup -> source_dir_backup -> dest_dir_backup -> file_pattern_1
        transfer_tasks = [
            {"file": file1, "dir": source_dir_trafico, "local_dir": dest_dir_trafico, "pattern": file_pattern_2},
            {"file": file2, "dir": source_dir_backup, "local_dir": dest_dir_backup, "pattern": file_pattern_1}
        ]

        downloaded_files = []
        
        for task in transfer_tasks:
            filename = task["file"]
            directory = task["dir"]
            local_dir = task["local_dir"]
            
            print(f"Cambiando al directorio remoto: {directory}")
            try:
                sftp_source.chdir(directory)
            except IOError as e:
                print(f"Error cambiando al directorio {directory}: {e}")
                continue

            # Asegurar que el directorio local existe
            if not os.path.exists(local_dir):
                print(f"Creando directorio local: {local_dir}")
                os.makedirs(local_dir, exist_ok=True)

            local_filename = os.path.join(local_dir, filename)
            print(f"Descargando {filename} desde origen a {local_filename}...")
            try:
                sftp_source.get(filename, local_filename)
                downloaded_files.append(local_filename)
                print(f"Descargado exitosamente: {local_filename}")
            except IOError as e:
                print(f"Error descargando {filename}: {e}")
                if os.path.exists(local_filename):
                    os.remove(local_filename)

        if not downloaded_files:
            print("No se descargaron archivos.")
        
        # 2. Eliminar archivos ANTIGUOS del Origen
        print("Iniciando limpieza de archivos antiguos en origen...")
        
        for task in transfer_tasks:
            directory = task["dir"]
            pattern = task["pattern"]
            
            # Solo intentamos borrar si el patrón parece dinámico (contiene %)
            if '%' in pattern:
                print(f"Cambiando al directorio para limpieza: {directory}")
                try:
                    sftp_source.chdir(directory)
                    delete_old_files(sftp_source, pattern, now)
                except Exception as e:
                    print(f"Error en limpieza del directorio {directory}: {e}")
            else:
                print(f"Saltando limpieza para patrón estático: {pattern} en {directory}")

        sftp_source.close()
        if hasattr(sftp_source, 'custom_transport'):
            sftp_source.custom_transport.close()

        print("Proceso completado.")

    except Exception as e:
        print(f"Ocurrió un error crítico: {e}")

if __name__ == "__main__":
    main()
