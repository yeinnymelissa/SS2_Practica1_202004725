import os
import mysql.connector
import pandas as pd
import csv
import pyodbc

# Variables globales para la conexión y el cursor
conn = None
cursor = None

def conectar_bd():
    global conn, cursor
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=semi2;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

def cerrar_bd():
    global conn, cursor
    if cursor:
        cursor.close()
    if conn:
        conn.close()

def borrar_modelo():
    conectar_bd()
    try:
        
        cursor.execute("IF OBJECT_ID('dbo.FLIGHT', 'U') IS NOT NULL DROP TABLE dbo.FLIGHT;")
        cursor.execute("IF OBJECT_ID('dbo.FLIGHT_INFO', 'U') IS NOT NULL DROP TABLE dbo.FLIGHT_INFO;")
        cursor.execute("IF OBJECT_ID('dbo.AIRPORT', 'U') IS NOT NULL DROP TABLE dbo.AIRPORT;")
        cursor.execute("IF OBJECT_ID('dbo.PASSENGER', 'U') IS NOT NULL DROP TABLE dbo.PASSENGER;")
        
        conn.commit()
        print("Modelo borrado exitosamente.")

    except pyodbc.Error as err:
        print(f"Error: {err}")
        conn.rollback()

    finally:
        cerrar_bd()

def crear_modelo():
    global cursor
    global conn
    conectar_bd()

    try:
        
        cursor.execute("""
        IF OBJECT_ID('dbo.PASSENGER', 'U') IS NULL
        CREATE TABLE PASSENGER (
            PASSENGER_ID VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CS_AS PRIMARY KEY,
            FIRST_NAME VARCHAR(50),
            LAST_NAME VARCHAR(50),
            GENDER VARCHAR(10),
            AGE INT,
            NATIONALITY VARCHAR(50)
        );
        """)

        
        cursor.execute("""
        IF OBJECT_ID('dbo.AIRPORT', 'U') IS NULL
        CREATE TABLE AIRPORT (
            AIRPORT_ID INT PRIMARY KEY IDENTITY(1,1),
            NAME VARCHAR(100),
            COUNTRY_CODE VARCHAR(5),
            COUNTRY_NAME VARCHAR(100),
            AIRPORT_CONTINENT VARCHAR(50),
            CONTINENTS VARCHAR(50)
        );
        """)

        
        cursor.execute("""
        IF OBJECT_ID('dbo.FLIGHT_INFO', 'U') IS NULL
        CREATE TABLE FLIGHT_INFO (
            FLIGHT_INFO_ID INT PRIMARY KEY IDENTITY(1,1),
            PILOT_NAME VARCHAR(100),
            DEPARTURE_DATE DATE,
            ARRIVAL_AIRPORT VARCHAR(5),
            STATUS VARCHAR(20)
        );
        """)

        
        cursor.execute("""
        IF OBJECT_ID('dbo.FLIGHT', 'U') IS NULL
        CREATE TABLE FLIGHT (
            ID INT PRIMARY KEY IDENTITY(1,1),
            PASSENGER_ID VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CS_AS,
            AIRPORT_ID INT,
            FLIGHT_INFO_ID INT,
            FOREIGN KEY (PASSENGER_ID) REFERENCES PASSENGER(PASSENGER_ID),
            FOREIGN KEY (AIRPORT_ID) REFERENCES AIRPORT(AIRPORT_ID),
            FOREIGN KEY (FLIGHT_INFO_ID) REFERENCES FLIGHT_INFO(FLIGHT_INFO_ID)
        );
        """)

        conn.commit()
        print("Modelo creado con éxito.")

    except pyodbc.Error as err:
        print(f"Error: {err}")
        conn.rollback()

    finally:
        cerrar_bd()
        cursor = None
        conn = None
        

def extraer_informacion():
    archivo = input("Ingrese la ruta del archivo CSV: ")
    print(f"Extrayendo información de {archivo}...")

    try:

        if conn is None:
            conectar_bd()

       
        cursor.execute("""
        IF OBJECT_ID('dbo.TEMP_DATA', 'U') IS NOT NULL
            DROP TABLE dbo.TEMP_DATA;
        CREATE TABLE TEMP_DATA (
            PASSENGER_ID VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CS_AS,
            FIRST_NAME VARCHAR(50),
            LAST_NAME VARCHAR(50),
            GENDER VARCHAR(10),
            AGE INT,
            NATIONALITY VARCHAR(50),
            NAME VARCHAR(100),
            COUNTRY_CODE VARCHAR(5),
            COUNTRY_NAME VARCHAR(100),
            AIRPORT_CONTINENT VARCHAR(50),
            CONTINENTS VARCHAR(50),
            DEPARTURE_DATE DATE,
            ARRIVAL_AIRPORT VARCHAR(5),
            PILOT_NAME VARCHAR(100),
            STATUS VARCHAR(20)
        );
        """)

        with open(archivo, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                arrival_airport = row.get('Arrival Airport', '')

                if arrival_airport in ['0', '-']:
                    continue

                def convertir_fecha(fecha_str):
                    formatos_fecha = ['%d-%m-%Y', '%m/%d/%Y', '%Y-%m-%d']
                    for formato in formatos_fecha:
                        try:
                            return pd.to_datetime(fecha_str, format=formato, errors='coerce').strftime('%Y-%m-%d')
                        except:
                            continue
                    return None

                departure_date = convertir_fecha(row.get('Departure Date', ''))
                values = (
                    row.get('Passenger ID', ''),
                    row.get('First Name', ''),
                    row.get('Last Name', ''),
                    row.get('Gender', ''),
                    row.get('Age', ''),
                    row.get('Nationality', ''),
                    row.get('Airport Name', ''),
                    row.get('Country Code', ''),
                    row.get('Country Name', ''),
                    row.get('Airport Continent', ''),
                    row.get('Continents', ''),
                    departure_date,
                    arrival_airport,
                    row.get('Pilot Name', ''),
                    row.get('Flight Status', '')
                )

                cursor.execute("""
                INSERT INTO TEMP_DATA (
                    PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY,
                    NAME, COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS,
                    DEPARTURE_DATE, ARRIVAL_AIRPORT, PILOT_NAME, STATUS
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, values)

        conn.commit()
        print("Información extraída correctamente.")
    
    except Exception as e:
        print(f"Error al leer el archivo: {e}")

def cargar_informacion():
    global conn
    if conn is None:
        print("Para cargar la información primero debe realizar la extracción de datos.")
        return
    
    try:
        cursor.execute("""
        MERGE INTO PASSENGER AS target
        USING (
            SELECT DISTINCT PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY
            FROM TEMP_DATA
            WHERE PASSENGER_ID IS NOT NULL
        ) AS source
        ON target.PASSENGER_ID = source.PASSENGER_ID
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY)
            VALUES (source.PASSENGER_ID, source.FIRST_NAME, source.LAST_NAME, source.GENDER, source.AGE, source.NATIONALITY);
        """)

        cursor.execute("""
        MERGE INTO AIRPORT AS target
        USING (
            SELECT DISTINCT NAME, COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS
            FROM TEMP_DATA
            WHERE NAME IS NOT NULL
        ) AS source
        ON target.NAME = source.NAME
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (NAME, COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS)
            VALUES (source.NAME, source.COUNTRY_CODE, source.COUNTRY_NAME, source.AIRPORT_CONTINENT, source.CONTINENTS);
        """)

        cursor.execute("""
        MERGE INTO FLIGHT_INFO AS target
        USING (
            SELECT DISTINCT PILOT_NAME, DEPARTURE_DATE, ARRIVAL_AIRPORT, STATUS
            FROM TEMP_DATA
            WHERE PILOT_NAME IS NOT NULL
        ) AS source
        ON target.PILOT_NAME = source.PILOT_NAME
           AND target.DEPARTURE_DATE = source.DEPARTURE_DATE
           AND target.ARRIVAL_AIRPORT = source.ARRIVAL_AIRPORT
           AND target.STATUS = source.STATUS
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (PILOT_NAME, DEPARTURE_DATE, ARRIVAL_AIRPORT, STATUS)
            VALUES (source.PILOT_NAME, source.DEPARTURE_DATE, source.ARRIVAL_AIRPORT, source.STATUS);
        """)

        cursor.execute("""
        MERGE INTO FLIGHT AS target
        USING (
        SELECT DISTINCT t.PASSENGER_ID, a.AIRPORT_ID, f.FLIGHT_INFO_ID
        FROM TEMP_DATA t
        JOIN PASSENGER p ON t.PASSENGER_ID = p.PASSENGER_ID
        JOIN AIRPORT a ON t.NAME = a.NAME
        JOIN FLIGHT_INFO f ON t.PILOT_NAME = f.PILOT_NAME
        AND t.DEPARTURE_DATE = f.DEPARTURE_DATE
        AND t.ARRIVAL_AIRPORT = f.ARRIVAL_AIRPORT
        AND t.STATUS = f.STATUS
        WHERE t.PASSENGER_ID IS NOT NULL
        ) AS source
        ON target.PASSENGER_ID = source.PASSENGER_ID
        AND target.AIRPORT_ID = source.AIRPORT_ID
        AND target.FLIGHT_INFO_ID = source.FLIGHT_INFO_ID
        WHEN NOT MATCHED BY TARGET THEN
        INSERT (PASSENGER_ID, AIRPORT_ID, FLIGHT_INFO_ID)
        VALUES (source.PASSENGER_ID, source.AIRPORT_ID, source.FLIGHT_INFO_ID);
        """)

        cursor.execute("TRUNCATE TABLE TEMP_DATA;")

        conn.commit()
        print("Información cargada correctamente.")
    
    except pyodbc.Error as err:
        print(f"Error: {err}")
        conn.rollback()
    
    finally:
        cerrar_bd()

def guardarConsulta(consulta, archivo, titulo):
    conectar_bd()
    cursor.execute(consulta)
    resultados = cursor.fetchall()
    
    with open(archivo, "a") as file:
        file.write(f"\n{titulo}\n")
        file.write("-" * len(titulo) + "\n")
        for fila in resultados:
            file.write(" | ".join(map(str, fila)) + "\n")
    
    cerrar_bd()

def realizar_consultas():
    archivo = "resultados_consultas.txt"
    
    # Limpiar el archivo si ya existe
    with open(archivo, "w") as file:
        file.write("Resultados de Consultas\n")
        file.write("=" * 30 + "\n")
    
    # Consulta 1: Contar Registros en Todas las Tablas
    consulta1 = """
    SELECT 'PASSENGER' AS Tabla, COUNT(*) AS Conteo FROM PASSENGER
    UNION ALL
    SELECT 'AIRPORT' AS Tabla, COUNT(*) AS Conteo FROM AIRPORT
    UNION ALL
    SELECT 'FLIGHT_INFO' AS Tabla, COUNT(*) AS Conteo FROM FLIGHT_INFO
    UNION ALL
    SELECT 'FLIGHT' AS Tabla, COUNT(*) AS Conteo FROM FLIGHT;
    """
    guardarConsulta(consulta1, archivo, "Conteo de Registros en Todas las Tablas")

    # Consulta 2: Porcentaje de Pasajeros por Género
    consulta2 = """
    SELECT GENDER, 
           (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM PASSENGER)) AS Porcentaje
    FROM PASSENGER
    GROUP BY GENDER;
    """
    guardarConsulta(consulta2, archivo, "Porcentaje de Pasajeros por Género")

    # Consulta 3: Nacionalidades con Su Mes y Año de Mayor Fecha de Salida
    consulta3 = """
    WITH FlightData AS (
    SELECT 
        p.NATIONALITY,
        FORMAT(fi.DEPARTURE_DATE, 'yyyy-MM') AS [Month_Year]
    FROM 
        FLIGHT f
    JOIN 
        PASSENGER p ON f.PASSENGER_ID = p.PASSENGER_ID
    JOIN 
        FLIGHT_INFO fi ON f.FLIGHT_INFO_ID = fi.FLIGHT_INFO_ID
)
SELECT 
    NATIONALITY AS [Nacionalidad],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-01' THEN 1 ELSE 0 END), 0) AS [2022-01],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-02' THEN 1 ELSE 0 END), 0) AS [2022-02],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-03' THEN 1 ELSE 0 END), 0) AS [2022-03],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-04' THEN 1 ELSE 0 END), 0) AS [2022-04],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-05' THEN 1 ELSE 0 END), 0) AS [2022-05],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-06' THEN 1 ELSE 0 END), 0) AS [2022-06],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-07' THEN 1 ELSE 0 END), 0) AS [2022-07],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-08' THEN 1 ELSE 0 END), 0) AS [2022-08],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-09' THEN 1 ELSE 0 END), 0) AS [2022-09],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-10' THEN 1 ELSE 0 END), 0) AS [2022-10],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-11' THEN 1 ELSE 0 END), 0) AS [2022-11],
    ISNULL(SUM(CASE WHEN [Month_Year] = '2022-12' THEN 1 ELSE 0 END), 0) AS [2022-12]
FROM 
    FlightData
GROUP BY 
    NATIONALITY
ORDER BY 
    NATIONALITY;
    """
    guardarConsulta(consulta3, archivo, "Nacionalidades con Su Mes y Año de Mayor Fecha de Salida")

    # Consulta 4: Contar Vuelos por País
    consulta4 = """
    SELECT a.COUNTRY_NAME, COUNT(*) AS TotalVuelos
    FROM FLIGHT f
    JOIN AIRPORT a ON f.AIRPORT_ID = a.AIRPORT_ID
    GROUP BY a.COUNTRY_NAME;
    """
    guardarConsulta(consulta4, archivo, "Conteo de Vuelos por País")

    # Consulta 5: Top 5 Aeropuertos con Mayor Número de Pasajeros
    consulta5 = """
    SELECT TOP 5 a.NAME AS Aeropuerto, COUNT(f.PASSENGER_ID) AS TotalPasajeros
    FROM FLIGHT f
    JOIN AIRPORT a ON f.AIRPORT_ID = a.AIRPORT_ID
    GROUP BY a.NAME
    ORDER BY TotalPasajeros DESC;
    """
    guardarConsulta(consulta5, archivo, "Top 5 Aeropuertos con Mayor Número de Pasajeros")

    # Consulta 6: Conteo de Vuelos por Estado de Vuelo
    consulta6 = """
    SELECT STATUS, COUNT(*) AS TotalVuelos
    FROM FLIGHT_INFO
    GROUP BY STATUS;
    """
    guardarConsulta(consulta6, archivo, "Conteo de Vuelos por Estado de Vuelo")

    # Consulta 7: Top 5 de los Países Más Visitados
    consulta7 = """
    SELECT TOP 5 a.COUNTRY_NAME AS País, COUNT(*) AS TotalVisitas
    FROM FLIGHT f
    JOIN AIRPORT a ON f.AIRPORT_ID = a.AIRPORT_ID
    GROUP BY a.COUNTRY_NAME
    ORDER BY TotalVisitas DESC;
    """
    guardarConsulta(consulta7, archivo, "Top 5 de los Países Más Visitados")

    # Consulta 8: Top 5 de los Continentes Más Visitados
    consulta8 = """
    SELECT TOP 5 a.AIRPORT_CONTINENT AS Continente, COUNT(*) AS TotalVisitas
    FROM FLIGHT f
    JOIN AIRPORT a ON f.AIRPORT_ID = a.AIRPORT_ID
    GROUP BY a.AIRPORT_CONTINENT
    ORDER BY TotalVisitas DESC;
    """
    guardarConsulta(consulta8, archivo, "Top 5 de los Continentes Más Visitados")

    # Consulta 9: Top 5 de Edades Divididas por Género que Más Viajan
    consulta9 = """
    SELECT TOP 5 p.GENDER AS Género, p.AGE AS Edad, COUNT(*) AS TotalPasajeros
    FROM PASSENGER p
    JOIN FLIGHT f ON p.PASSENGER_ID = f.PASSENGER_ID
    GROUP BY p.GENDER, p.AGE
    ORDER BY TotalPasajeros DESC;
    """
    guardarConsulta(consulta9, archivo, "Top 5 de Edades Divididas por Género que Más Viajan")

    # Consulta 10: Conteo de Vuelos por Mes y Año
    consulta10 = """
    SELECT FORMAT(fi.DEPARTURE_DATE, 'MM-yyyy') AS MesAño, COUNT(*) AS TotalVuelos
    FROM FLIGHT f
    JOIN FLIGHT_INFO fi ON f.FLIGHT_INFO_ID = fi.FLIGHT_INFO_ID
    GROUP BY FORMAT(fi.DEPARTURE_DATE, 'MM-yyyy');
    """
    guardarConsulta(consulta10, archivo, "Conteo de Vuelos por Mes y Año")

    print("Todas las consultas se han ejecutado y los resultados están guardados en 'resultados_consultas.txt'.")


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
