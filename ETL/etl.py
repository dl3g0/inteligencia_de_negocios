import mysql.connector
from datetime import datetime, timedelta, time
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# --- Configuración de Bases de Datos se leerán de Key Vault ---
KEY_VAULT_URL = os.environ.get("KEY_VAULT_URL")

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

def get_db_credentials(db_prefix):
    """Recupera las credenciales de la base de datos desde Azure Key Vault."""
    try:
        host = secret_client.get_secret(f"{db_prefix}-HOST").value
        user = secret_client.get_secret(f"{db_prefix}-USER").value
        password = secret_client.get_secret(f"{db_prefix}-PASSWORD").value
        database = secret_client.get_secret(f"{db_prefix}-DATABASE").value
        print(f"Credenciales para {db_prefix} recuperadas exitosamente.")
        return {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'ssl_ca': os.environ.get("MYSQL_SSL_CA") # Para Azure MySQL Flexible Server, se espera la ruta del certificado CA
        }
    except Exception as e:
        print(f"Error al recuperar credenciales para {db_prefix} de Key Vault: {e}")
        return None

# Recuperar credenciales para origen y almacén
DB_CONFIG_ORIGEN = get_db_credentials("DB-ORIGEN")
DB_CONFIG_ALMACEN = get_db_credentials("DB-ALMACEN")

if not KEY_VAULT_URL:
    print("Error: La variable de entorno KEY_VAULT_URL no está configurada. Terminando.")
    exit(1)

if not DB_CONFIG_ORIGEN or not DB_CONFIG_ALMACEN:
    print("No se pudieron cargar las credenciales de la base de datos desde Key Vault. Terminando ETL.")
    exit(1)

# --- Funciones de Conexión ---
def connect_db(config):
    """Establece conexión con la base de datos."""
    try:
        # Añadir ssl_ca si está presente en la configuración para conexiones SSL
        if 'ssl_ca' in config and config['ssl_ca']:
            conn = mysql.connector.connect(
                host=config['host'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                ssl_ca=config['ssl_ca']
            )
        else:
            # Si ssl_ca no está definido o es None, intenta la conexión sin SSL_CA explícito
            conn = mysql.connector.connect(
                host=config['host'],
                user=config['user'],
                password=config['password'],
                database=config['database']
            )
        
        if conn.is_connected():
            print(f"Conexión exitosa a la base de datos: {config['database']}")
            return conn
    except mysql.connector.Error as err:
        print(f"Error al conectar a la base de datos {config['database']}: {err}")
        return None

# --- Funciones ETL  ---

def extract_data(conn_origen, table_name):
    """Extrae todos los datos de una tabla de la base de datos de origen."""
    cursor = conn_origen.cursor(dictionary=True) # Retorna filas como diccionarios
    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Error al extraer datos de {table_name}: {err}")
        return []
    finally:
        cursor.close()

def truncate_warehouse_tables(conn_almacen):
    """Truncar tablas del almacén para una carga limpia (Full Load)."""
    cursor = conn_almacen.cursor()
    try:
        print("Truncando tablas del almacén para una carga limpia...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;") # Deshabilitar FKs temporalmente
        cursor.execute("TRUNCATE TABLE citas_hechos;")
        cursor.execute("TRUNCATE TABLE dim_tiempo;")
        cursor.execute("TRUNCATE TABLE dim_medicos;")
        cursor.execute("TRUNCATE TABLE dim_pacientes;")
        cursor.execute("TRUNCATE TABLE dim_especialidades;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;") # Habilitar FKs
        conn_almacen.commit()
        print("Tablas del almacén truncadas exitosamente.")
    except mysql.connector.Error as err:
        conn_almacen.rollback()
        print(f"Error al truncar tablas del almacén: {err}")
    finally:
        cursor.close()

def load_dim_especialidades(conn_almacen, especialidades_origen):
    """Carga y transforma datos para dim_especialidades."""
    cursor = conn_almacen.cursor()
    print("Cargando dim_especialidades...")
    try:
        for esp in especialidades_origen:
            # Transforma y asegura que los IDs sean VARCHAR(250)
            id_especialidad_sk = str(esp['EspecialidadID'])
            id_especialidad_bk = str(esp['EspecialidadID']) # id_especialidad como Business Key

            cursor.execute(
                "INSERT IGNORE INTO dim_especialidades (id_especialidad_sk, id_especialidad, nombre_especialidad, fecha_carga) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)",
                (id_especialidad_sk, id_especialidad_bk, esp['NombreEspecialidad'])
            )
        conn_almacen.commit()
        print(f"Cargadas {cursor.rowcount} filas en dim_especialidades.")
    except mysql.connector.Error as err:
        conn_almacen.rollback()
        print(f"Error al cargar dim_especialidades: {err}")
    finally:
        cursor.close()

def load_dim_pacientes(conn_almacen, pacientes_origen):
    """Carga y transforma datos para dim_pacientes."""
    cursor = conn_almacen.cursor()
    print("Cargando dim_pacientes...")
    try:
        for pac in pacientes_origen:
            # Transforma y asegura que los IDs sean VARCHAR(250)
            id_paciente_sk = str(pac['PacienteID'])
            id_paciente_bk = str(pac['PacienteID']) # id_paciente como Business Key

            cursor.execute(
                "INSERT IGNORE INTO dim_pacientes (id_paciente_sk, id_paciente, apellido, direccion, fecha_nacimiento, genero, nombre, telefono, fecha_carga) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                (id_paciente_sk, id_paciente_bk, pac['Apellido'], pac['Direccion'], pac['FechaNacimiento'],
                 pac['Genero'], pac['Nombre'], pac['Telefono'])
            )
        conn_almacen.commit()
        print(f"Cargadas {cursor.rowcount} filas en dim_pacientes.")
    except mysql.connector.Error as err:
        conn_almacen.rollback()
        print(f"Error al cargar dim_pacientes: {err}")
    finally:
        cursor.close()

def load_dim_medicos(conn_almacen, medicos_origen):
    """Carga y transforma datos para dim_medicos."""
    cursor = conn_almacen.cursor()
    print("Cargando dim_medicos...")
    try:
        for med in medicos_origen:
            # Transforma y asegura que los IDs sean VARCHAR(250)
            id_medico_sk = str(med['MedicoID'])
            id_medico_bk = str(med['MedicoID']) # id_medico como Business Key
            id_especialidad_bk = str(med['EspecialidadID']) # Clave de negocio de especialidad

            cursor.execute(
                "INSERT IGNORE INTO dim_medicos (id_medico_sk, id_medico, id_especialidad, codigo_empleado, nombre, apellido, genero, fecha_carga) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                (id_medico_sk, id_medico_bk, id_especialidad_bk, med['CodigoEmpleado'], med['Nombre'], med['Apellido'], med['Genero'])
            )
        conn_almacen.commit()
        print(f"Cargadas {cursor.rowcount} filas en dim_medicos.")
    except mysql.connector.Error as err:
        conn_almacen.rollback()
        print(f"Error al cargar dim_medicos: {err}")
    finally:
        cursor.close()

def load_dim_tiempo(conn_almacen, citas_origen):
    """Carga y transforma datos para dim_tiempo a partir de las fechas de citas."""
    cursor = conn_almacen.cursor()
    print("Cargando dim_tiempo...")
    unique_dates = set()
    rows_to_insert = []

    for cita in citas_origen:
        hora_cita_raw = cita['HoraCita']
        
        # Convertir timedelta a datetime.time si es necesario
        if isinstance(hora_cita_raw, timedelta):
            total_seconds = hora_cita_raw.seconds
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            hora_cita_obj = time(hours, minutes, seconds)
        elif isinstance(hora_cita_raw, time):
            hora_cita_obj = hora_cita_raw
        else:
            # En caso de otro tipo inesperado, intentar parsear o loggear error
            print(f"Advertencia: Tipo de HoraCita inesperado para cita {cita.get('CitaID')}: {type(hora_cita_raw)}. Se espera datetime.time o timedelta.")
            try:
                # Si viene como string 'HH:MM:SS', se puede parsear
                if isinstance(hora_cita_raw, str):
                    hora_cita_obj = datetime.strptime(hora_cita_raw, '%H:%M:%S').time()
                else:
                    # Si no es un tipo esperado, saltar o manejar el error
                    print(f"Error: HoraCita no convertible para cita {cita.get('CitaID')}. Saltando fila.")
                    continue
            except ValueError as ve:
                print(f"Error al parsear HoraCita string para cita {cita.get('CitaID')}: {ve}. Saltando fila.")
                continue

        # Combinar fecha y hora para una marca de tiempo completa
        fecha_hora_cita = datetime.combine(cita['FechaCita'], hora_cita_obj)
        
        # Generar id_tiempo_sk basado en el formatoYYYYMMDDHHMMSS (VARCHAR)
        id_tiempo_sk = fecha_hora_cita.strftime('%Y%m%d%H%M%S')

        if id_tiempo_sk not in unique_dates:
            unique_dates.add(id_tiempo_sk)
            rows_to_insert.append((
                id_tiempo_sk,
                fecha_hora_cita.date(),
                fecha_hora_cita.year,
                fecha_hora_cita.month,
                fecha_hora_cita.day,
                fecha_hora_cita.hour,
                fecha_hora_cita.minute,
                fecha_hora_cita.second,
                fecha_hora_cita.strftime('%B'), # Nombre del mes
                fecha_hora_cita.strftime('%A')  # Nombre del día de la semana
            ))
    
    if rows_to_insert:
        try:
            # Usar executemany para inserciones más eficientes
            insert_query = """
            INSERT IGNORE INTO dim_tiempo (id_tiempo_sk, fecha, anio, mes, dia, hora, minuto, segundo, nombre_mes, dia_semana, fecha_carga)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            cursor.executemany(insert_query, rows_to_insert)
            conn_almacen.commit()
            print(f"Cargadas {cursor.rowcount} filas únicas en dim_tiempo.")
        except mysql.connector.Error as err:
            conn_almacen.rollback()
            print(f"Error al cargar dim_tiempo: {err}")
        finally:
            cursor.close()
    else:
        print("No hay nuevas entradas de tiempo para cargar.")


def load_citas_hechos(conn_almacen, citas_origen):
    """Carga y transforma datos para citas_hechos, buscando claves sustitutas."""
    cursor_almacen = conn_almacen.cursor()
    print("Cargando citas_hechos...")
    rows_to_insert = []

    # Recuperar mapeos de claves de negocio a claves sustitutas de las dimensiones
    def get_sk_mapping(table_name, pk_col_sk, bk_col_name):
        cursor_map = conn_almacen.cursor()
        cursor_map.execute(f"SELECT {pk_col_sk}, {bk_col_name} FROM {table_name}")
        # Aseguramos que tanto SK como BK sean cadenas
        mapping = {str(row[1]): str(row[0]) for row in cursor_map.fetchall()} 
        cursor_map.close()
        return mapping

    paciente_sk_map = get_sk_mapping('dim_pacientes', 'id_paciente_sk', 'id_paciente')
    medico_sk_map = get_sk_mapping('dim_medicos', 'id_medico_sk', 'id_medico')
    
    for cita in citas_origen:
        try:
            # Obtener claves sustitutas usando los mapeos
            # Aseguramos que la clave de negocio de la cita también sea cadena para la búsqueda
            id_paciente_sk = paciente_sk_map.get(str(cita['PacienteID']))
            id_medico_sk = medico_sk_map.get(str(cita['MedicoID']))
            
            # Asegurar que HoraCita sea un objeto time para combinar correctamente
            hora_cita_raw = cita['HoraCita']
            if isinstance(hora_cita_raw, timedelta):
                total_seconds = hora_cita_raw.seconds
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                hora_cita_obj = time(hours, minutes, seconds)
            elif isinstance(hora_cita_raw, time):
                hora_cita_obj = hora_cita_raw
            else:
                print(f"Advertencia: Tipo de HoraCita inesperado para cita {cita.get('CitaID')} en hechos: {type(hora_cita_raw)}. Saltando fila.")
                continue


            # id_tiempo_sk se deriva de la fecha_hora_cita y ya es una cadena
            fecha_hora_cita_obj = datetime.combine(cita['FechaCita'], hora_cita_obj)
            id_tiempo_sk = fecha_hora_cita_obj.strftime('%Y%m%d%H%M%S')

            if id_paciente_sk and id_medico_sk and id_tiempo_sk:
                rows_to_insert.append((
                    str(cita['CitaID']), # id_cita como VARCHAR(250)
                    id_paciente_sk,
                    id_medico_sk,
                    id_tiempo_sk,
                    fecha_hora_cita_obj, # Fecha y hora completas (DATETIME)
                    cita['EstadoCita'],
                    cita['MotivoCita']
                ))
            else:
                print(f"Advertencia: No se pudo encontrar SK para cita {cita.get('CitaID')} (PacienteID: {cita['PacienteID']}, MedicoID: {cita['MedicoID']}). Saltando.")

        except KeyError as ke:
            print(f"Error de mapeo de clave para cita {cita.get('CitaID')}: {ke}. Asegúrate que las claves de negocio existen en las dimensiones.")
            continue # Saltar esta fila si hay un problema de clave

    if rows_to_insert:
        try:
            insert_query = """
            INSERT INTO citas_hechos (id_cita, id_paciente_sk, id_medico_sk, id_tiempo_sk, fecha_hora_cita, estado_cita, motivo_cita, fecha_carga)
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            cursor_almacen.executemany(insert_query, rows_to_insert)
            conn_almacen.commit()
            print(f"Cargadas {cursor_almacen.rowcount} filas en citas_hechos.")
        except mysql.connector.Error as err:
            conn_almacen.rollback()
            print(f"Error al cargar citas_hechos: {err}")
        finally:
            cursor_almacen.close()
    else:
        print("No hay citas para cargar en la tabla de hechos.")


# --- Función Principal de Orquestación ETL ---
def run_etl_process():
    """Ejecuta el proceso ETL completo."""
    conn_origen = None
    conn_almacen = None
    try:
        # Verificar que las credenciales se cargaron correctamente
        if not DB_CONFIG_ORIGEN or not DB_CONFIG_ALMACEN:
            print("Las credenciales de la base de datos no están disponibles. Terminando ETL.")
            return

        conn_origen = connect_db(DB_CONFIG_ORIGEN)
        conn_almacen = connect_db(DB_CONFIG_ALMACEN)

        if not conn_origen or not conn_almacen:
            print("No se pudo establecer conexión con una o ambas bases de datos. Terminando ETL.")
            return

        # 1. Truncar tablas del almacén antes de iniciar (para Full Load)
        truncate_warehouse_tables(conn_almacen)

        # 2. Extraer datos de origen
        print("\nIniciando extracción de datos de origen...")
        pacientes_origen = extract_data(conn_origen, 'Pacientes')
        medicos_origen = extract_data(conn_origen, 'Medicos')
        especialidades_origen = extract_data(conn_origen, 'Especialidades')
        citas_origen = extract_data(conn_origen, 'Citas')
        print("Extracción de datos de origen completada.")

        # 3. Cargar Dimensiones (Orden importante: Especialidades -> Medicos -> Pacientes -> Tiempo)
        print("\nIniciando carga de dimensiones...")
        load_dim_especialidades(conn_almacen, especialidades_origen)
        load_dim_pacientes(conn_almacen, pacientes_origen)
        load_dim_medicos(conn_almacen, medicos_origen)
        load_dim_tiempo(conn_almacen, citas_origen)
        print("Carga de dimensiones completada.")

        # 4. Cargar Tabla de Hechos
        print("\nIniciando carga de tabla de hechos...")
        load_citas_hechos(conn_almacen, citas_origen)
        print("Carga de tabla de hechos completada.")

        print("\nProceso ETL finalizado exitosamente.")

    finally:
        if conn_origen:
            conn_origen.close()
            print("Conexión a origen_citas cerrada.")
        if conn_almacen:
            conn_almacen.close()
            print("Conexión a almacen_citas cerrada.")

if __name__ == "__main__":
    run_etl_process()
