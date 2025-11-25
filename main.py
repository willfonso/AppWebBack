from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import mysql.connector
from datetime import datetime
from decimal import Decimal
from fastapi.responses import StreamingResponse
import pandas as pd
import io

app = FastAPI(title="API Ventas G24")

# CORS para permitir tu PWA
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# 🔥 FUNCIÓN DE CONEXIÓN
# -----------------------------
def get_connection():
    return mysql.connector.connect(
        host="190.228.29.68",
        user="g24",
        password="Enero01..",
        database="saig24"
    )

# -----------------------------
# 🔥 SERIALIZADOR DECIMAL
# -----------------------------
def fix_decimals(row):
    clean = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            clean[k] = int(v)
        else:
            clean[k] = v
    return clean

# -----------------------------
# 🔥 ENDPOINT PRINCIPAL /ventas
# -----------------------------
@app.get("/ventas")
def get_ventas(
    desde: str = Query(...),
    hasta: str = Query(...),
    grupo: str | None = Query(None),
    pdv: str | None = Query(None),
):

    # Detectar año automáticamente
    año = desde.split("-")[0]
    tabla = f"aaFactur_A{año}"

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        filtros = []

        filtros.append(f"DATE(a.FECHA) BETWEEN '{desde}' AND '{hasta}'")

        if grupo:
            filtros.append(f"b.GRUPO = '{grupo}'")

        if pdv:
            filtros.append(f"a.WEB_SUCURSAL = '{pdv}'")

        where_sql = " AND ".join(filtros)

        # -----------------------
        # 🔥 LÓGICA REAL DE TUS DATOS
        # -----------------------
        sql = f"""
        SELECT  
            b.NOMBRE AS PDV,

            SUM(CASE WHEN a.REFERENCIA = 'AR' THEN a.UNITARIO ELSE 0 END) AS ARREGLOS,

            SUM(CASE
                    WHEN a.TARJETA='' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS EFEC,

            SUM(
                CASE 
                    WHEN a.REFERENCIA='CR' AND (a.TARJETA='MP' OR a.TARJETA='MERCADO') THEN -a.UNITARIO
                    WHEN a.REFERENCIA!='AR' AND (a.TARJETA='MP' OR a.TARJETA='MERCADO') THEN a.UNITARIO
                    ELSE 0
                END
            ) AS MP,
            SUM(CASE
                    WHEN a.TARJETA='RAPPI' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='RAPPI' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS RAPPI,
            SUM(CASE
                    WHEN a.TARJETA='PYT' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='PYT' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS PYT,
            SUM(CASE
                    WHEN a.TARJETA='PYE' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='PYE' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS PYE,
            SUM(CASE
                    WHEN a.TARJETA='TDB' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='TDB' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS TDB,
            SUM(CASE
                    WHEN a.TARJETA='TDC' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='TDC' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS TDC,
            SUM(CASE
                    WHEN a.TARJETA='BITCOIN' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='BITCOIN' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS BITCOIN,
            SUM(CASE
                    WHEN a.TARJETA='DELIVERY' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='DELIVERY' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS DELIVERY,

            SUM(
                CASE 
                    WHEN a.REFERENCIA='CR' THEN -a.UNITARIO
                    ELSE a.UNITARIO
                END
            ) AS CTA_CORRIENTE,

            SUM(
                CASE 
                    WHEN a.REFERENCIA='CR' THEN -a.UNITARIO
                    ELSE a.UNITARIO
                END
            ) AS TOTAL

        FROM saig24.{tabla} AS a
        LEFT JOIN saig24.aarchivos AS b ON a.WEB_SUCURSAL = b.CODIGO

        LEFT JOIN saig24.{tabla} AS g ON a.WEB_SUCURSAL = g.WEB_SUCURSAL AND a.NRO_CIERRE = g.NRO_CIERRE AND DATE(g.FECHA) = date_add('{desde}', INTERVAL -1 day)        
        WHERE b.estado='ACTIVO' AND {where_sql} AND g.WEB_SUCURSAL IS NULL
        
              
        GROUP BY a.WEB_SUCURSAL
        ORDER BY b.NOMBRE ASC;
        """

        cursor.execute(sql)
        rows = cursor.fetchall()

        result = [fix_decimals(r) for r in rows]

        cursor.close()
        conn.close()

        return JSONResponse({"data": result})

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/pdvs")
def obtener_pdvs():
    """
    Devuelve todos los puntos de venta activos desde saig24.aarchivos
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT 
                CODIGO AS pdv,
                NOMBRE AS nombre
            FROM saig24.aarchivos
            WHERE estado = 'ACTIVO'
            ORDER BY nombre;
        """)

        datos = cursor.fetchall()

        cursor.close()
        conn.close()

        return {"data": datos}

    except Exception as e:
        return {"error": str(e)}

# main.py - Agrega esto al final del archivo

# -----------------------------
# 🔥 ENDPOINT DE LOGIN
# -----------------------------
@app.post("/login")
def login_user(
    username: str = Query(...),
    password: str = Query(...)
):
    """
    Verifica las credenciales del usuario en la tabla abempleados_usua.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # ⚠️ IMPORTANTE: El campo 'password' en tu DB es INT(15). 
        # Asegúrate de que el valor que usas para la comparación sea un entero.
        try:
            password_int = int(password)
        except ValueError:
            return JSONResponse({"error": "La contraseña debe ser numérica."}, status_code=400)
            
        sql = """
            SELECT 
                dni, nivel, username
            FROM saig24.abempleados_usua
            WHERE username = %s AND password = %s
        """
        
        # Ejecutamos la consulta con parámetros seguros para evitar inyección SQL
        cursor.execute(sql, (username, password_int))
        user = cursor.fetchone()

        cursor.close()
        conn.close()
        
        if user:
            # Autenticación exitosa
            return JSONResponse({"message": "Login exitoso", "user": user})
        else:
            # Autenticación fallida
            return JSONResponse({"error": "Usuario o contraseña inválidos"}, status_code=401)

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# main.py - Agrega este código al final

# -----------------------------
# 🔥 ENDPOINT /export/excel
# -----------------------------
@app.get("/export/excel")
def exportar_ventas_excel(
    desde: str = Query(...),
    hasta: str = Query(...),
    grupo: str | None = Query(None),
    pdv: str | None = Query(None),
):

    # Utilizamos la misma lógica de conexión y SQL que /ventas
    año = desde.split("-")[0]
    tabla = f"aaFactur_A{año}"
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        filtros = []
        filtros.append(f"DATE(a.FECHA) BETWEEN '{desde}' AND '{hasta}'")

        if grupo:
            filtros.append(f"b.GRUPO = '{grupo}'")

        if pdv:
            filtros.append(f"a.WEB_SUCURSAL = '{pdv}'")

        where_sql = " AND ".join(filtros)

        # ⚠️ IMPORTANTE: REUSA AQUÍ LA LÓGICA SQL COMPLETA Y CORREGIDA DE /VENTAS
        # Asegúrate de que las columnas EFEC y CTA_CORRIENTE sean las corregidas.
        # Por simplicidad, aquí se usa una versión genérica.
        sql = f"""
        SELECT  
            b.NOMBRE AS PDV,

            SUM(CASE WHEN a.REFERENCIA = 'AR' THEN a.UNITARIO ELSE 0 END) AS ARREGLOS,

            SUM(CASE
                    WHEN a.TARJETA='' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS EFEC,

            SUM(
                CASE 
                    WHEN a.REFERENCIA='CR' AND (a.TARJETA='MP' OR a.TARJETA='MERCADO') THEN -a.UNITARIO
                    WHEN a.REFERENCIA!='AR' AND (a.TARJETA='MP' OR a.TARJETA='MERCADO') THEN a.UNITARIO
                    ELSE 0
                END
            ) AS MP,
            SUM(CASE
                    WHEN a.TARJETA='RAPPI' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='RAPPI' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS RAPPI,
            SUM(CASE
                    WHEN a.TARJETA='PYT' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='PYT' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS PYT,
            SUM(CASE
                    WHEN a.TARJETA='PYE' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='PYE' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS PYE,
            SUM(CASE
                    WHEN a.TARJETA='TDB' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='TDB' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS TDB,
            SUM(CASE
                    WHEN a.TARJETA='TDC' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='TDC' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS TDC,
            SUM(CASE
                    WHEN a.TARJETA='BITCOIN' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='BITCOIN' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS BITCOIN,
            SUM(CASE
                    WHEN a.TARJETA='DELIVERY' AND a.REFERENCIA='CR' THEN -a.UNITARIO
                    WHEN a.TARJETA='DELIVERY' AND a.REFERENCIA='FA' THEN a.UNITARIO ELSE 0 
                END
            ) AS DELIVERY,

            SUM(
                CASE 
                    WHEN a.REFERENCIA='CR' THEN -a.UNITARIO
                    ELSE a.UNITARIO
                END
            ) AS CTA_CORRIENTE,

            SUM(
                CASE 
                    WHEN a.REFERENCIA='CR' THEN -a.UNITARIO
                    ELSE a.UNITARIO
                END
            ) AS TOTAL

        FROM saig24.{tabla} AS a
        LEFT JOIN saig24.aarchivos AS b ON a.WEB_SUCURSAL = b.CODIGO

        LEFT JOIN saig24.{tabla} AS g ON a.WEB_SUCURSAL = g.WEB_SUCURSAL AND a.NRO_CIERRE = g.NRO_CIERRE AND DATE(g.FECHA) = date_add('{desde}', INTERVAL -1 day)        
        WHERE b.estado='ACTIVO' AND {where_sql} AND g.WEB_SUCURSAL IS NULL
        
              
        GROUP BY a.WEB_SUCURSAL
        ORDER BY b.NOMBRE ASC;
        """

        cursor.execute(sql)
        rows = cursor.fetchall()
        
        # 1. Convertir resultados a DataFrame de pandas
        df = pd.DataFrame(rows)

        columnas_a_entero = [
            'ARREGLOS', 'EFEC', 'MP', 'RAPPI', 'PYT', 'PYE', 'TDB', 'TDC', 
            'BITCOIN', 'DELIVERY', 'CTA_CORRIENTE', 'TOTAL'
        ]
        
        for col in columnas_a_entero:
            if col in df.columns:
                # Usamos .fillna(0) para manejar posibles valores NULL antes de la conversión a int
                df[col] = df[col].fillna(0).astype(int)

        # 2. Crear un buffer en memoria para el archivo Excel
        excel_buffer = io.BytesIO()
        
        # 3. Escribir el DataFrame en el buffer como Excel
        df.to_excel(excel_buffer, index=False, sheet_name='Reporte_Ventas')
        excel_buffer.seek(0)
        
        # 4. Configurar el nombre del archivo de salida
        filename = f"Ventas_desde_{desde}_hasta_{hasta}.xlsx"

        # 5. Devolver la respuesta al cliente
        return StreamingResponse(
            content=excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    except Exception as e:
        return JSONResponse({"error": f"Error al generar Excel: {str(e)}"}, status_code=500)