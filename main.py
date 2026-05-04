from fastapi import FastAPI
import mysql.connector
from mysql.connector import Error

app = FastAPI()

# Datos de la Data Tier (VM de tu compañero)
db_config = {
    'host': '10.0.0.X',  # <--- COLOCA AQUÍ LA IP PRIVADA DE LA VM DE BASE DE DATOS
    'user': 'Orfeadmin',
    'password': 'tu_password',
    'database': 'orfeshop'
}

@app.get("/")
def inicio():
    return {"mensaje": "Bienvenido a la API de OrfeShop - Capa de Aplicación operativa"}

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