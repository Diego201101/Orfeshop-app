import os
from fastapi import FastAPI
import mysql.connector
from mysql.connector import Error

app = FastAPI()

# Datos de la Data Tier (VM de tu compañero)
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

@app.get("/")
def inicio():
    return {"mensaje": "Bienvenido a la API de OrfeShop - Capa de Aplicacion operativa"}

@app.get("/probar_db")
def probar_db():
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            return {"status": "Éxito", "mensaje": "Conexión establecida con la Data Tier en Azure"}
    except Error as e:
        return {"status": "Error", "mensaje": f"No se pudo conectar: {str(e)}"}
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()


@app.get("/ver_tablas")
def ver_tablas():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        cursor = connection.cursor()
        # Consulta para listar las tablas
        cursor.execute("SHOW TABLES;")
        tablas = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return {
            "status": "Éxito",
            "tablas_encontradas": [t[0] for t in tablas]
        }
    except Error as e:
        return {"status": "Error", "mensaje": str(e)}


@app.get("/tabla/{nombre_tabla}")
def mostrar_tabla(nombre_tabla: str):
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        cursor = connection.cursor(dictionary=True)
        # Nota: En producción se debe validar 'nombre_tabla' para evitar SQL Injection
        cursor.execute(f"SELECT * FROM {nombre_tabla};")
        datos = cursor.fetchall()
        cursor.close()
        connection.close()
        return {"tabla": nombre_tabla, "filas": len(datos), "contenido": datos}
    except Error as e:
        return {"status": "Error", "mensaje": str(e)}


# 1. Ver productos con poco stock (Menos de 5 unidades)
@app.get("/consultas/bajo_stock")
def bajo_stock():
    query = "SELECT nombre, stock FROM productos WHERE stock < 5;"
    return ejecutar_query(query)

# 2. Ver los 5 productos más caros
@app.get("/consultas/productos_premium")
def productos_caros():
    query = "SELECT nombre, precio FROM productos ORDER BY precio DESC LIMIT 5;"
    return ejecutar_query(query)

# 3. Listar clientes registrados recientemente
@app.get("/consultas/clientes_nuevos")
def clientes_nuevos():
    query = "SELECT nombre, email FROM clientes ORDER BY id DESC LIMIT 10;"
    return ejecutar_query(query)

# 4. Total de ventas (Suma de precios)
@app.get("/consultas/total_ventas")
def total_ventas():
    query = "SELECT SUM(total) as gran_total FROM ventas;"
    return ejecutar_query(query)

# 5. Buscar un producto por nombre (ejemplo: /consultas/buscar?nombre=torta)
@app.get("/consultas/buscar")
def buscar_producto(nombre: str):
    query = f"SELECT * FROM productos WHERE nombre LIKE '%{nombre}%';"
    return ejecutar_query(query)

# Función auxiliar para no repetir código
def ejecutar_query(sql):
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'), database=os.getenv('DB_NAME')
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql)
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        return res
    except Error as e:
        return {"error": str(e)}
