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

# 1. Ver categorías (Ya vimos que esta sí existe)
@app.get("/consultas/categorias")
def ver_categorias():
    # Ajusta 'Categoria' según lo que viste en el SHOW TABLES
    query = "SELECT * FROM Categoria;" 
    return ejecutar_query(query)

# 2. Productos más caros (Ajusta 'Producto' y 'precio' según tu tabla)
@app.get("/consultas/top_precios")
def productos_caros():
    # Ejemplo: Si tu tabla se llama 'Producto' en vez de 'productos'
    query = "SELECT * FROM Producto ORDER BY precio DESC LIMIT 5;"
    return ejecutar_query(query)

# 3. Listar lo que hay en una tabla específica (Ej. Clientes o Usuarios)
@app.get("/consultas/usuarios")
def ver_usuarios():
    query = "SELECT nombre, correo FROM Usuario LIMIT 10;"
    return ejecutar_query(query)

# 4. Conteo total de una tabla (Ej. ¿Cuántas ventas hay?)
@app.get("/consultas/conteo_ventas")
def total_ventas():
    query = "SELECT COUNT(*) as total FROM Venta;"
    return ejecutar_query(query)

# 5. Consulta combinada (Si tienes llaves foráneas)
@app.get("/api/consultas/productos_con_categoria")
def productos_detalle():
    # Ejemplo de un JOIN sencillo
    query = """
        SELECT p.nombre, c.nombre as categoria 
        FROM Producto p 
        JOIN Categoria c ON p.id_categoria = c.id 
        LIMIT 10;
    """
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
