import os
from fastapi import FastAPI
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import time
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI()

# CORS - Configuración amplia para Azure
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://20.119.200.194",
        "http://20.119.200.194:80",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "http://52.247.13.228"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración de base de datos
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'auth_plugin': 'mysql_native_password'
}

# ============= MODELOS =============
class LoginSchema(BaseModel):
    correo: str
    password: str

class RegistroSchema(BaseModel):
    nombre: str
    apellidoPaterno: str
    apellidoMaterno: str
    correo: str
    telefono: str
    password: str

class ProductoCompra(BaseModel):
    idProducto: int
    cantidad: int

class Compra(BaseModel):
    idUsuario: int
    idMetodoPago: int
    tipoComprobante: str
    costoTotal: float
    ruc: str = ""
    razonSocial: str = ""
    direccionEnvio: str = ""
    productos: List[dict]

class ActualizarEstadoPedido(BaseModel):
    estadoEnvio: str
    observacion: str = ""

class ActualizarMetodoPago(BaseModel):
    idMetodoPago: int

class AnularPedido(BaseModel):
    motivo: str

class AgregarProductoPedido(BaseModel):
    idProducto: int
    cantidad: int

class ConvertirAFactura(BaseModel):
    RUC: str
    razonSocial: str
    nombreEmpresa: str = ""

# ============= FUNCIÓN AUXILIAR =============
def ejecutar_query(sql, params=None):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or ())
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        return res
    except Error as e:
        return {"error": str(e)}

def ejecutar_update(sql, params=None):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        conn.commit()
        afectadas = cursor.rowcount
        cursor.close()
        conn.close()
        return {"success": True, "filas_afectadas": afectadas}
    except Error as e:
        return {"success": False, "error": str(e)}

# ============= ENDPOINTS BÁSICOS (GET) =============
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
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES;")
        tablas = cursor.fetchall()
        cursor.close()
        connection.close()
        return {"status": "Éxito", "tablas_encontradas": [t[0] for t in tablas]}
    except Error as e:
        return {"status": "Error", "mensaje": str(e)}

@app.get("/tabla/{nombre_tabla}")
def mostrar_tabla(nombre_tabla: str):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {nombre_tabla};")
        datos = cursor.fetchall()
        cursor.close()
        connection.close()
        return {"tabla": nombre_tabla, "filas": len(datos), "contenido": datos}
    except Error as e:
        return {"status": "Error", "mensaje": str(e)}

@app.get("/consultas/categorias")
def ver_categorias():
    return ejecutar_query("SELECT * FROM Categoria;")

@app.get("/consultas/top_precios")
def productos_caros():
    return ejecutar_query("SELECT * FROM Producto ORDER BY precio DESC LIMIT 5;")

@app.get("/consultas/usuarios")
def ver_usuarios():
    return ejecutar_query("SELECT idUsuarioCliente, nombre, correo FROM usuarioCliente LIMIT 10;")

@app.get("/consultas/conteo_ventas")
def total_ventas():
    return ejecutar_query("SELECT COUNT(*) as total FROM Pedido;")

@app.get("/consultas/productos_con_categoria")
def productos_detalle():
    return ejecutar_query("""
        SELECT p.nombre, c.nombre as categoria, p.precio
        FROM Producto p 
        JOIN Categoria c ON p.idCategoria = c.idCategoria 
        LIMIT 10;
    """)

# ============= ENDPOINTS DE AUTENTICACIÓN =============
@app.post("/login")
def login(data: LoginSchema):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        query = "SELECT idUsuarioCliente, nombre, apellidoPaterno, apellidoMaterno, correo, telefono FROM usuarioCliente WHERE correo = %s AND password = %s"
        cursor.execute(query, (data.correo, data.password))
        usuario = cursor.fetchone()
        cursor.close()
        connection.close()

        if usuario:
            return {
                "success": True,
                "user": {
                    "id": usuario['idUsuarioCliente'],
                    "nombre": f"{usuario['nombre']} {usuario['apellidoPaterno']}",
                    "correo": usuario['correo'],
                    "telefono": usuario['telefono']
                }
            }
        else:
            return {"success": False, "message": "Credenciales incorrectas"}
    except Error as e:
        return {"success": False, "message": f"Error: {str(e)}"}

@app.post("/registro")
def registrar_usuario(data: RegistroSchema):
    if len(data.password) <= 6:
        return {"success": False, "message": "La contraseña debe tener al menos 6 caracteres"}
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("SELECT MAX(idUsuarioCliente) FROM usuarioCliente")
        max_id = cursor.fetchone()[0]
        nuevo_id = (max_id + 1) if max_id else 1

        query = """
            INSERT INTO usuarioCliente 
            (idUsuarioCliente, nombre, apellidoPaterno, apellidoMaterno, correo, telefono, password) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (nuevo_id, data.nombre, data.apellidoPaterno,
                               data.apellidoMaterno, data.correo, data.telefono, data.password))

        connection.commit()
        cursor.close()
        connection.close()
        return {"success": True, "message": "Usuario registrado", "id": nuevo_id}
    except Error as e:
        return {"success": False, "message": str(e)}

# ============= ENDPOINTS DE COMPRAS Y PEDIDOS =============
@app.post("/comprar")
def realizar_compra(compra: Compra):
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("SELECT MAX(idPedido) FROM Pedido")
        max_id_pedido = cursor.fetchone()[0]
        nuevo_id = (max_id_pedido + 1) if max_id_pedido else 1

        # Insertar Pedido
        cursor.execute("INSERT INTO Pedido (idPedido, idUsuarioCliente, fechaPedido, costoTotal) VALUES (%s, %s, NOW(), %s)",
                       (nuevo_id, compra.idUsuario, float(compra.costoTotal)))

        # Insertar Pago
        cursor.execute("INSERT INTO Pago (idPago, idPedido, fechaPago, monto, estadoPago, codigoTransaccion) VALUES (%s, %s, NOW(), %s, 'Completado', %s)",
                       (nuevo_id, nuevo_id, float(compra.costoTotal), f"TRX-{nuevo_id}-{int(time.time())}"))

        # Insertar metodoPago
        nombre_metodo = {1: "Tarjeta de Crédito/Débito", 2: "Transferencia Bancaria", 3: "Yape / Plin"}.get(compra.idMetodoPago, "Otro")
        cursor.execute("INSERT INTO metodoPago (idmetodoPago, idPago, nombreMetodo, descripcionMetodoPago, estado) VALUES (%s, %s, %s, %s, 'Activo')",
                       (nuevo_id, nuevo_id, nombre_metodo, f"Pago procesado con {nombre_metodo}"))

        # Insertar Comprobante
        cursor.execute("INSERT INTO Comprobante (idComprobante, idmetodoPago, tipoComprobante) VALUES (%s, %s, %s)",
                       (nuevo_id, nuevo_id, compra.tipoComprobante))

        # Insertar detallePedido
        for i, prod in enumerate(compra.productos):
            id_det = nuevo_id * 1000 + i + 1
            cursor.execute("SELECT precio FROM Producto WHERE idProducto = %s", (prod.idProducto,))
            resultado = cursor.fetchone()
            if resultado:
                precio = float(resultado[0])
                subtotal = precio * prod.cantidad
                igv = subtotal * 0.18
                cursor.execute("""INSERT INTO detallePedido (idDetalle, idPedido, idProducto, cantidad, costoEnvio, IGV, subtotal) 
                                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                               (id_det, nuevo_id, prod.idProducto, prod.cantidad, 15.00, igv, subtotal))

        # Insertar Envio
        cursor.execute("INSERT INTO Envio (idEnvio, idPedido, direccionEnvio, fechaEnvio, estadoEnvio) VALUES (%s, %s, %s, NOW(), 'Pendiente')",
                       (nuevo_id + 1000, nuevo_id, compra.direccionEnvio))

        # Si es Factura
        if compra.tipoComprobante == "Factura" and compra.ruc:
            cursor.execute("INSERT INTO Factura (idFactura, idComprobante, RUC, nombreEmpresa, razonSocial) VALUES (%s, %s, %s, %s, %s)",
                           (nuevo_id, nuevo_id, compra.ruc, compra.razonSocial, compra.razonSocial))

        connection.commit()
        return {"success": True, "idPedido": nuevo_id}

    except Exception as e:
        if connection:
            connection.rollback()
        return {"success": False, "message": str(e)}
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

@app.get("/usuarios/{id_usuario}/pedidos")
def obtener_pedidos_usuario(id_usuario: int):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        query = """
            SELECT p.idPedido, p.fechaPedido, p.costoTotal, e.estadoEnvio, c.tipoComprobante, c.idmetodoPago, mp.nombreMetodo as nombreMetodoPago
            FROM Pedido p
            LEFT JOIN Envio e ON p.idPedido = e.idPedido
            LEFT JOIN Comprobante c ON p.idPedido = c.idComprobante
            LEFT JOIN metodoPago mp ON c.idmetodoPago = mp.idmetodoPago
            WHERE p.idUsuarioCliente = %s
            ORDER BY p.idPedido DESC
        """
        cursor.execute(query, (id_usuario,))
        pedidos = cursor.fetchall()

        for pedido in pedidos:
            cursor.execute("SELECT dp.cantidad, dp.costoEnvio, dp.subtotal, p.nombre, p.precio FROM detallePedido dp JOIN Producto p ON dp.idProducto = p.idProducto WHERE dp.idPedido = %s",
                           (pedido['idPedido'],))
            pedido['productos'] = cursor.fetchall()

            if pedido['tipoComprobante'] == 'Factura':
                cursor.execute("SELECT RUC, razonSocial FROM Factura WHERE idComprobante = %s", (pedido['idPedido'],))
                factura = cursor.fetchone()
                if factura:
                    pedido['RUC'] = factura['RUC']
                    pedido['razonSocial'] = factura['razonSocial']

        cursor.close()
        connection.close()
        return pedidos
    except Exception as e:
        return []

# ============= ENDPOINTS POST PARA POSTMAN =============
@app.post("/api/pedidos/nuevo")
def crear_pedido_manual(compra: Compra):
    return realizar_compra(compra)

@app.post("/api/pedidos/{id_pedido}/productos")
def agregar_producto_pedido(id_pedido: int, producto: AgregarProductoPedido):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("SELECT costoTotal FROM Pedido WHERE idPedido = %s", (id_pedido,))
        pedido = cursor.fetchone()
        if not pedido:
            return {"success": False, "message": "Pedido no encontrado"}

        cursor.execute("SELECT precio FROM Producto WHERE idProducto = %s", (producto.idProducto,))
        resultado = cursor.fetchone()
        if not resultado:
            return {"success": False, "message": "Producto no encontrado"}

        precio = float(resultado[0])
        subtotal_nuevo = precio * producto.cantidad
        igv_nuevo = subtotal_nuevo * 0.18
        nuevo_total = float(pedido[0]) + subtotal_nuevo + 15.00

        cursor.execute("SELECT MAX(idDetalle) FROM detallePedido")
        max_detalle = cursor.fetchone()[0]
        nuevo_id_detalle = (max_detalle + 1) if max_detalle else 1

        cursor.execute("""INSERT INTO detallePedido (idDetalle, idPedido, idProducto, cantidad, costoEnvio, IGV, subtotal) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                       (nuevo_id_detalle, id_pedido, producto.idProducto, producto.cantidad, 15.00, igv_nuevo, subtotal_nuevo))

        cursor.execute("UPDATE Pedido SET costoTotal = %s WHERE idPedido = %s", (nuevo_total, id_pedido))

        connection.commit()
        cursor.close()
        connection.close()
        return {"success": True, "message": f"Producto agregado al pedido #{id_pedido}", "nuevo_total": nuevo_total}
    except Exception as e:
        return {"success": False, "message": str(e)}

# ============= ENDPOINTS PUT PARA POSTMAN =============
@app.put("/api/pedidos/{id_pedido}/estado")
def actualizar_estado_envio(id_pedido: int, estado: ActualizarEstadoPedido):
    estados_validos = ['Pendiente', 'Preparando', 'Enviado', 'Entregado', 'Cancelado']
    if estado.estadoEnvio not in estados_validos:
        return {"success": False, "message": f"Estado inválido. Permitidos: {estados_validos}"}

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("SELECT idPedido FROM Pedido WHERE idPedido = %s", (id_pedido,))
        if not cursor.fetchone():
            return {"success": False, "message": "Pedido no encontrado"}

        cursor.execute("UPDATE Envio SET estadoEnvio = %s WHERE idPedido = %s", (estado.estadoEnvio, id_pedido))

        if cursor.rowcount == 0:
            cursor.execute("SELECT MAX(idEnvio) FROM Envio")
            max_id = cursor.fetchone()[0]
            nuevo_id_envio = (max_id + 1) if max_id else 1
            cursor.execute("INSERT INTO Envio (idEnvio, idPedido, estadoEnvio, fechaEnvio) VALUES (%s, %s, %s, NOW())",
                           (nuevo_id_envio, id_pedido, estado.estadoEnvio))

        connection.commit()
        cursor.close()
        connection.close()
        return {"success": True, "message": f"Estado de pedido #{id_pedido} actualizado a '{estado.estadoEnvio}'"}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.put("/api/pedidos/{id_pedido}/metodo-pago")
def cambiar_metodo_pago(id_pedido: int, metodo: ActualizarMetodoPago):
    nombres_metodos = {1: "Tarjeta de Crédito/Débito", 2: "Transferencia Bancaria", 3: "Yape / Plin"}

    if metodo.idMetodoPago not in nombres_metodos:
        return {"success": False, "message": "Método inválido. Opciones: 1, 2, 3"}

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("SELECT idPedido FROM Pedido WHERE idPedido = %s", (id_pedido,))
        if not cursor.fetchone():
            return {"success": False, "message": "Pedido no encontrado"}

        cursor.execute("UPDATE Comprobante SET idmetodoPago = %s WHERE idComprobante = %s", (metodo.idMetodoPago, id_pedido))
        nuevo_nombre = nombres_metodos[metodo.idMetodoPago]
        cursor.execute("UPDATE metodoPago SET nombreMetodo = %s, descripcionMetodoPago = %s WHERE idmetodoPago = %s",
                       (nuevo_nombre, f"Método actualizado a {nuevo_nombre}", id_pedido))

        connection.commit()
        cursor.close()
        connection.close()
        return {"success": True, "message": f"Método de pago actualizado a '{nuevo_nombre}'"}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.put("/api/pedidos/{id_pedido}/convertir-factura")
def convertir_boleta_factura(id_pedido: int, datos_factura: ConvertirAFactura):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        cursor.execute("SELECT tipoComprobante FROM Comprobante WHERE idComprobante = %s", (id_pedido,))
        comprobante = cursor.fetchone()

        if not comprobante:
            return {"success": False, "message": "Pedido sin comprobante"}
        if comprobante['tipoComprobante'] == 'Factura':
            return {"success": False, "message": "Ya es Factura"}
        if comprobante['tipoComprobante'] != 'Boleta':
            return {"success": False, "message": f"Tipo actual: {comprobante['tipoComprobante']}. Solo Boleta puede convertirse"}

        cursor.execute("SELECT estadoEnvio FROM Envio WHERE idPedido = %s", (id_pedido,))
        envio = cursor.fetchone()
        if envio and envio['estadoEnvio'] == 'Cancelado':
            return {"success": False, "message": "Pedido cancelado no se puede convertir"}

        if len(datos_factura.RUC) != 11:
            return {"success": False, "message": "RUC debe tener 11 dígitos"}

        cursor.execute("UPDATE Comprobante SET tipoComprobante = 'Factura' WHERE idComprobante = %s", (id_pedido,))
        nombre_empresa = datos_factura.nombreEmpresa if datos_factura.nombreEmpresa else datos_factura.razonSocial
        cursor.execute("INSERT INTO Factura (idFactura, idComprobante, RUC, nombreEmpresa, razonSocial) VALUES (%s, %s, %s, %s, %s)",
                       (id_pedido, id_pedido, datos_factura.RUC, nombre_empresa, datos_factura.razonSocial))

        connection.commit()
        cursor.close()
        connection.close()
        return {"success": True, "message": f"Pedido #{id_pedido} convertido a Factura"}
    except Exception as e:
        return {"success": False, "message": str(e)}

# ============= ENDPOINTS DELETE PARA POSTMAN =============
@app.delete("/api/pedidos/{id_pedido}")
def anular_pedido(id_pedido: int, motivo: AnularPedido = None):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("SELECT idPedido, costoTotal FROM Pedido WHERE idPedido = %s", (id_pedido,))
        pedido = cursor.fetchone()
        if not pedido:
            return {"success": False, "message": "Pedido no encontrado"}

        motivo_texto = motivo.motivo if motivo and motivo.motivo else "No especificado"

        cursor.execute("UPDATE Envio SET estadoEnvio = 'Cancelado' WHERE idPedido = %s", (id_pedido,))
        cursor.execute("UPDATE Pago SET estadoPago = 'Cancelado' WHERE idPedido = %s", (id_pedido,))

        connection.commit()
        cursor.close()
        connection.close()
        return {"success": True, "message": f"Pedido #{id_pedido} cancelado", "motivo": motivo_texto, "monto_original": float(pedido[1])}
    except Exception as e:
        return {"success": False, "message": str(e)}

@app.delete("/api/pedidos/{id_pedido}/productos/{id_producto}")
def eliminar_producto_pedido(id_pedido: int, id_producto: int):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("SELECT idDetalle, subtotal FROM detallePedido WHERE idPedido = %s AND idProducto = %s", (id_pedido, id_producto))
        detalle = cursor.fetchone()

        if not detalle:
            return {"success": False, "message": "Producto no encontrado"}

        id_detalle = detalle[0]
        subtotal_eliminado = float(detalle[1])

        cursor.execute("SELECT costoTotal FROM Pedido WHERE idPedido = %s", (id_pedido,))
        costo_actual = float(cursor.fetchone()[0])
        nuevo_costo = costo_actual - subtotal_eliminado

        cursor.execute("DELETE FROM detallePedido WHERE idDetalle = %s", (id_detalle,))
        cursor.execute("UPDATE Pedido SET costoTotal = %s WHERE idPedido = %s", (nuevo_costo, id_pedido))

        connection.commit()
        cursor.close()
        connection.close()
        return {"success": True, "message": f"Producto eliminado", "nuevo_total": nuevo_costo}
    except Exception as e:
        return {"success": False, "message": str(e)}

# ============= ENDPOINTS ADICIONALES GET =============
@app.get("/api/pedidos/todos")
def obtener_todos_pedidos():
    return ejecutar_query("""
        SELECT p.idPedido, p.fechaPedido, p.costoTotal, e.estadoEnvio, c.tipoComprobante, mp.nombreMetodo as metodoPago
        FROM Pedido p
        LEFT JOIN Envio e ON p.idPedido = e.idPedido
        LEFT JOIN Comprobante c ON p.idPedido = c.idComprobante
        LEFT JOIN metodoPago mp ON c.idmetodoPago = mp.idmetodoPago
        ORDER BY p.idPedido DESC
    """)

@app.get("/api/facturas/listado")
def obtener_facturas():
    return ejecutar_query("""
        SELECT p.idPedido, p.fechaPedido, p.costoTotal, f.RUC, f.razonSocial, uc.nombre, uc.apellidoPaterno
        FROM Pedido p
        JOIN Comprobante c ON p.idPedido = c.idComprobante
        JOIN Factura f ON c.idComprobante = f.idComprobante
        JOIN usuarioCliente uc ON p.idUsuarioCliente = uc.idUsuarioCliente
        WHERE c.tipoComprobante = 'Factura'
        ORDER BY p.idPedido DESC
    """)

@app.get("/api/boletas/listado")
def obtener_boletas():
    return ejecutar_query("""
        SELECT p.idPedido, p.fechaPedido, p.costoTotal, uc.nombre, uc.apellidoPaterno
        FROM Pedido p
        JOIN Comprobante c ON p.idPedido = c.idComprobante
        JOIN usuarioCliente uc ON p.idUsuarioCliente = uc.idUsuarioCliente
        WHERE c.tipoComprobante = 'Boleta'
        ORDER BY p.idPedido DESC
    """)

@app.get("/api/pedidos/{id_pedido}/auditoria")
def ver_auditoria_pedido(id_pedido: int):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        cursor.execute("""SELECT p.idPedido, p.fechaPedido, p.costoTotal, uc.nombre, uc.apellidoPaterno, uc.correo
                          FROM Pedido p JOIN usuarioCliente uc ON p.idUsuarioCliente = uc.idUsuarioCliente
                          WHERE p.idPedido = %s""", (id_pedido,))
        pedido = cursor.fetchone()

        if not pedido:
            return {"success": False, "message": "Pedido no encontrado"}

        cursor.execute("SELECT estadoEnvio, direccionEnvio FROM Envio WHERE idPedido = %s", (id_pedido,))
        pedido['envio'] = cursor.fetchone()

        cursor.execute("SELECT tipoComprobante FROM Comprobante WHERE idComprobante = %s", (id_pedido,))
        pedido['comprobante'] = cursor.fetchone()

        cursor.execute("SELECT RUC, razonSocial FROM Factura WHERE idComprobante = %s", (id_pedido,))
        factura = cursor.fetchone()
        if factura:
            pedido['factura'] = factura

        cursor.execute("SELECT nombre, cantidad, precio, subtotal FROM detallePedido dp JOIN Producto p ON dp.idProducto = p.idProducto WHERE dp.idPedido = %s", (id_pedido,))
        pedido['productos'] = cursor.fetchall()

        cursor.close()
        connection.close()
        return {"success": True, "auditoria": pedido}
    except Exception as e:
        return {"success": False, "error": str(e)}
