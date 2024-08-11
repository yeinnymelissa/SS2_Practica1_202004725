import os
import mysql.connector

def borrar_modelo():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="ktalan30",
        database="semi2"
    )
    
    cursor = conn.cursor()

    try:
        cursor.execute("DROP TABLE IF EXISTS FLIGHT;")
        cursor.execute("DROP TABLE IF EXISTS FLIGHT_INFO;")
        cursor.execute("DROP TABLE IF EXISTS AIRPORT;")
        cursor.execute("DROP TABLE IF EXISTS PASSENGER;")
        
        conn.commit()
        print("Modelo borrado exitosamente.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def crear_modelo():
   
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="ktalan30",
        database="semi2"
    )
    
    cursor = conn.cursor()

    # Creación de la tabla de Pasajeros (PASSENGER)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS PASSENGER (
        PASSENGER_ID VARCHAR(10) PRIMARY KEY,
        FIRST_NAME VARCHAR(50),
        LAST_NAME VARCHAR(50),
        GENDER VARCHAR(10),
        AGE INT,
        NATIONALITY VARCHAR(50)
    );
    """)

    # Creación de la tabla de Aeropuertos (AIRPORT)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS AIRPORT (
        AIRPORT_ID INT PRIMARY KEY AUTO_INCREMENT,
        NAME VARCHAR(100),
        COUNTRY_CODE VARCHAR(5),
        COUNTRY_NAME VARCHAR(100),
        AIRPORT_CONTINENT VARCHAR(50),
        CONTINENTS VARCHAR(50)
    );
    """)

    # Creación de la tabla de Información de Vuelo (FLIGHT_INFO)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS FLIGHT_INFO (
        FLIGHT_INFO_ID INT PRIMARY KEY AUTO_INCREMENT,
        PILOT_NAME VARCHAR(100),
        DEPARTURE_DATE DATE,
        ARRIVAL_AIRPORT VARCHAR(5),
        STATUS VARCHAR(20)
    );
    """)

    # Creación de la tabla de Vuelos (FLIGHT)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS FLIGHT (
        ID INT PRIMARY KEY AUTO_INCREMENT,
        PASSENGER_ID VARCHAR(10),
        AIRPORT_ID INT,
        FLIGHT_INFO_ID INT,
        FOREIGN KEY (PASSENGER_ID) REFERENCES PASSENGER(PASSENGER_ID),
        FOREIGN KEY (AIRPORT_ID) REFERENCES AIRPORT(AIRPORT_ID),
        FOREIGN KEY (FLIGHT_INFO_ID) REFERENCES FLIGHT_INFO(FLIGHT_INFO_ID)
    );
    """)

    # Confirmar los cambios en la base de datos
    conn.commit()

    # Cerrar la conexión
    cursor.close()
    conn.close()
    print("Modelo creado con éxito.")

def extraer_informacion():
    ruta_archivos = input("Ingrese la ruta de los archivos de carga: ")
    print(f"Extrayendo información de {ruta_archivos}...")
    # Aquí iría el código para extraer la información de los archivos
    # ...

def cargar_informacion():
    print("Cargando información...")
    # Aquí iría el código para transformar y cargar la información en la base de datos
    # ...

def realizar_consultas():
    print("Realizando consultas...")
    # Aquí iría el código para realizar las consultas y guardar los resultados en un archivo de texto
    # ...

def menu():
    while True:
        print("\nMenú de opciones:")
        print("1. Borrar modelo")
        print("2. Crear modelo")
        print("3. Extraer información")
        print("4. Cargar información")
        print("5. Realizar consultas")
        print("6. Salir")
        
        opcion = input("Seleccione una opción: ")
        
        if opcion == '1':
            borrar_modelo()
        elif opcion == '2':
            crear_modelo()
        elif opcion == '3':
            extraer_informacion()
        elif opcion == '4':
            cargar_informacion()
        elif opcion == '5':
            realizar_consultas()
        elif opcion == '6':
            print("Saliendo...")
            break
        else:
            print("Opción no válida. Por favor, seleccione una opción del 1 al 6.")

if __name__ == "__main__":
    menu()
