from faker import Faker
import mysql.connector
from datetime import datetime, timedelta, time # Importar time
import random # ¡Importar el módulo random!

# Configuración de la conexión a la base de datos de origen
# Asegúrate de que estas credenciales sean correctas para tu DB de origen
config = {
   # comentado por seguridad
}

def connect_to_db():
    """Conecta a la base de datos MySQL."""
    try:
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            print("Conexión exitosa a la base de datos.")
            return connection
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def generate_data(num_pacientes, num_medicos, num_citas):
    """Genera datos de prueba para las tablas de origen."""
    fake = Faker('es_CO')
    especialidades_lista = ['Cardiología', 'Dermatología', 'Gastroenterología', 'Neurología', 'Pediatría', 
                            'Oftalmología', 'Traumatología', 'Urología', 'Ginecología', 'Psiquiatría']
    estado_cita_lista = ['Programada', 'Atendida', 'Cancelada', 'Reprogramada']
    motivo_cita_lista = ['Consulta general', 'Control', 'Urgencia', 'Examen']
    
    data = {
        'especialidades': [],
        'medicos': [],
        'pacientes': [],
        'citas': []
    }

    # Generar especialidades
    # En la DB de origen, EspecialidadID es AUTO_INCREMENT, así que no lo generamos aquí
    # Solo generamos el nombre y descripción
    for nombre_esp in especialidades_lista:
        data['especialidades'].append({
            'NombreEspecialidad': nombre_esp,
            'Descripcion': fake.sentence(nb_words=6)
        })

    # Generar médicos
    # MedicoID y EspecialidadID son AUTO_INCREMENT en el origen, así que los IDs se manejarán al insertar
    # Necesitamos un mapeo temporal de especialidades para las FK
    temp_especialidad_map = {nombre: i+1 for i, nombre in enumerate(especialidades_lista)}

    for i in range(num_medicos):
        # Asignar una especialidad existente
        nombre_especialidad_elegida = random.choice(especialidades_lista)
        # En este script, no podemos obtener el ID real de la especialidad hasta que se inserta
        # Por ahora, solo generamos los datos del médico
        data['medicos'].append({
            'Nombre': fake.first_name(),
            'Apellido': fake.last_name(),
            'CodigoEmpleado': fake.unique.bothify(text='EMP####???'), # Patrón más robusto
            'Genero': random.choice(['Masculino', 'Femenino']),
            'EspecialidadNombre': nombre_especialidad_elegida, # Usamos el nombre para el mapeo posterior
            'TelefonoContacto': fake.phone_number(),
            'Email': fake.email()
        })

    # Generar pacientes
    for i in range(num_pacientes):
        data['pacientes'].append({
            'Nombre': fake.first_name(),
            'Apellido': fake.last_name(),
            # Eliminado tzinfo=None de date_of_birth
            'FechaNacimiento': fake.date_of_birth(minimum_age=18, maximum_age=80), 
            'Genero': random.choice(['Masculino', 'Femenino']),
            'Direccion': fake.address(),
            'Telefono': fake.phone_number(),
            'Email': fake.email()
        })

    # Generar citas (los IDs de Paciente y Medico se obtendrán al insertar)
    for i in range(num_citas):
        fecha_cita = fake.date_between(start_date='-1y', end_date='+1y') 
        hora_cita = fake.time_object() # Genera un objeto datetime.time
        estado_cita = random.choice(estado_cita_lista)
        motivo_cita = random.choice(motivo_cita_lista)
        
        data['citas'].append({
            'FechaCita': fecha_cita,
            'HoraCita': hora_cita,
            'EstadoCita': estado_cita,
            'MotivoCita': motivo_cita,
            'FechaCreacion': datetime.now()
        })
    return data

def insert_data(connection, data):
    """Inserta los datos generados en la base de datos de origen."""
    cursor = connection.cursor()
    try:
        # Deshabilitar FKs temporalmente para inserciones en orden
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;") 
        
        # Truncar tablas para asegurar una inserción limpia (opcional, solo para pruebas)
        print("Truncando tablas de origen (Pacientes, Medicos, Especialidades, Citas)...")
        cursor.execute("TRUNCATE TABLE Citas;")
        cursor.execute("TRUNCATE TABLE Medicos;")
        cursor.execute("TRUNCATE TABLE Pacientes;")
        cursor.execute("TRUNCATE TABLE Especialidades;")
        
        # Habilitar FKs de nuevo antes de insertar si se truncó
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        # Almacenar IDs generados por AUTO_INCREMENT para FKs
        especialidades_id_map = {} # Mapea NombreEspecialidad a EspecialidadID
        medicos_id_list = []
        pacientes_id_list = []

        # 1. Insertar Especialidades
        print("Insertando especialidades...")
        especialidades_to_insert = []
        for especialidad in data['especialidades']:
            especialidades_to_insert.append((especialidad['NombreEspecialidad'], especialidad['Descripcion']))
        
        if especialidades_to_insert:
            insert_especialidades_query = "INSERT INTO Especialidades (NombreEspecialidad, Descripcion) VALUES (%s, %s)"
            cursor.executemany(insert_especialidades_query, especialidades_to_insert)
            connection.commit()
            
            # Recuperar IDs generados para especialidades
            cursor.execute("SELECT EspecialidadID, NombreEspecialidad FROM Especialidades")
            for row in cursor.fetchall():
                especialidades_id_map[row[1]] = row[0]
            print(f"Cargadas {len(data['especialidades'])} filas en Especialidades.")


        # 2. Insertar Pacientes
        print("Insertando pacientes...")
        pacientes_to_insert = []
        for paciente in data['pacientes']:
            pacientes_to_insert.append((
                paciente['Nombre'], paciente['Apellido'], paciente['FechaNacimiento'],
                paciente['Genero'], paciente['Direccion'], paciente['Telefono'], paciente['Email']
            ))
        
        if pacientes_to_insert:
            insert_pacientes_query = "INSERT INTO Pacientes (Nombre, Apellido, FechaNacimiento, Genero, Direccion, Telefono, Email) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(insert_pacientes_query, pacientes_to_insert)
            connection.commit()
            
            # Recuperar IDs generados para pacientes
            cursor.execute("SELECT PacienteID FROM Pacientes")
            pacientes_id_list = [row[0] for row in cursor.fetchall()]
            print(f"Cargadas {len(data['pacientes'])} filas en Pacientes.")


        # 3. Insertar Médicos
        print("Insertando médicos...")
        medicos_to_insert = []
        seen_codes = set() # Para detectar duplicados de CodigoEmpleado generados por Faker
        for medico in data['medicos']:
            especialidad_id = especialidades_id_map.get(medico['EspecialidadNombre'])
            if especialidad_id:
                code = medico['CodigoEmpleado']
                if code in seen_codes:
                    print(f"Advertencia: Código de empleado duplicado generado por Faker: {code}. Saltando médico.")
                    continue # Saltar este médico si su código ya fue visto
                seen_codes.add(code)

                medicos_to_insert.append((
                    medico['Nombre'], medico['Apellido'], code,
                    medico['Genero'], especialidad_id, medico['TelefonoContacto'], medico['Email']
                ))
            else:
                print(f"Advertencia: Especialidad '{medico['EspecialidadNombre']}' no encontrada para médico {medico['Nombre']} {medico['Apellido']}. Saltando médico.")
        
        if medicos_to_insert:
            insert_medicos_query = "INSERT INTO Medicos (Nombre, Apellido, CodigoEmpleado, Genero, EspecialidadID, TelefonoContacto, Email) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(insert_medicos_query, medicos_to_insert)
            connection.commit()
            
            # Recuperar IDs generados para médicos
            cursor.execute("SELECT MedicoID FROM Medicos")
            medicos_id_list = [row[0] for row in cursor.fetchall()]
            print(f"Cargadas {len(medicos_to_insert)} filas en Medicos.") # Usar len(medicos_to_insert) ya que algunos podrían haberse saltado


        # 4. Insertar Citas
        print("Insertando citas...")
        citas_to_insert = []
        if not pacientes_id_list or not medicos_id_list:
            print("Error: No hay pacientes o médicos para asignar citas. Saltando inserción de citas.")
        else:
            for cita in data['citas']:
                # Asignar IDs de paciente y médico reales de los insertados
                paciente_id = random.choice(pacientes_id_list)
                medico_id = random.choice(medicos_id_list)

                citas_to_insert.append((
                    paciente_id, medico_id, cita['FechaCita'], cita['HoraCita'],
                    cita['EstadoCita'], cita['MotivoCita'], cita['FechaCreacion']
                ))
            
            if citas_to_insert:
                insert_citas_query = "INSERT INTO Citas (PacienteID, MedicoID, FechaCita, HoraCita, EstadoCita, MotivoCita, FechaCreacion) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.executemany(insert_citas_query, citas_to_insert)
                connection.commit()
                print(f"Cargadas {len(data['citas'])} filas en Citas.")

        print("Datos insertados correctamente en la base de datos de origen.")

    except Exception as e:
        connection.rollback()
        print(f"Error al insertar datos: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    connection = connect_to_db()
    if connection:
        num_pacientes = 2000
        num_medicos = 200
        num_citas = 10000
        data = generate_data(num_pacientes, num_medicos, num_citas)
        insert_data(connection, data)
        connection.close()
