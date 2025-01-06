from flask import Flask, request, jsonify           # type: ignore
import os
import logging
from datetime import datetime

app = Flask(__name__)

# Archivo existente de taxis
ARCHIVO_TAXI = "taxi.txt"
ARCHIVO_TOKENS = "token.txt"
ARCHIVO_CLAVES = "claves.txt"
# Crear el archivo de taxis si no existe
if not os.path.exists(ARCHIVO_TAXI):
    with open(ARCHIVO_TAXI, "w") as archivo:
        archivo.write("")  # Crear un archivo vacío
        
logging.basicConfig(filename='audit_log.txt', level=logging.INFO, format='%(message)s')

def registrar_evento(origen_ip, accion, detalles):
    evento = {
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "source_ip": origen_ip,
        "event": accion,
        "details": detalles
    }
    logging.info(evento)

def leer_taxis():
    taxis = []
    try:
        with open(ARCHIVO_TAXI, "r") as archivo:
            for linea in archivo:
                datos = linea.strip().replace('<', '').split('>')
                if len(datos) >= 5:
                    taxis.append({
                        "asignado": datos[0],
                        "operativo": datos[1],
                        "taxi_id": datos[2],
                        "pos_x": int(datos[3]),
                        "pos_y": int(datos[4])
                    })
    except FileNotFoundError:
        print("Archivo de taxis no encontrado, se creará uno nuevo.")
    except Exception as e:
        print(f"Error al leer el archivo de taxis: {e}")
    return taxis

def guardar_taxis(taxis):
    try:
        with open(ARCHIVO_TAXI, "w") as archivo:
            for taxi in taxis:
                archivo.write(f"<{taxi['asignado']}><{taxi['operativo']}><{taxi['taxi_id']}><{taxi['pos_x']}><{taxi['pos_y']}>\n")
    except Exception as e:
        print(f"Error al guardar en el archivo de taxis: {e}")

@app.route("/register_taxi", methods=["POST"])
def register_taxi():
    origen_ip = request.remote_addr  # Obtiene la IP del cliente
    registrar_evento(origen_ip, "Intento de registro de taxi", {"taxi_id": request.json.get("taxi_id")})
    try:
        # Validar que se envían datos JSON
        if not request.json:
            return jsonify({"error": "No se enviaron datos JSON"}), 400

        # Obtener datos del taxi del cuerpo de la solicitud
        data = request.json
        taxi_id = data.get("taxi_id")
        pos_x = data.get("pos_x", 0)
        pos_y = data.get("pos_y", 0)

        if not taxi_id:
            return jsonify({"error": "Falta el ID del taxi"}), 400

        # Leer el archivo de taxis
        taxis = leer_taxis()

        # Verificar si el taxi ya está registrado
        for taxi in taxis:
            if taxi["taxi_id"] == taxi_id:
                # Si está activo, rechazar el registro
                if taxi["asignado"] == "SI" and taxi["operativo"] == "SI":
                    return jsonify({"error": f"El taxi {taxi_id} ya está registrado y operativo"}), 400

                # Si está inactivo, actualizar estado y posición
                taxi["asignado"] = "SI"
                taxi["operativo"] = "SI"
                taxi["pos_x"] = pos_x
                taxi["pos_y"] = pos_y

                # Guardar los cambios en el archivo
                guardar_taxis(taxis)
                return jsonify({"mensaje": f"Taxi {taxi_id} actualizado y está operativo"}), 200

        # Si no está registrado, devolver error
        return jsonify({"error": f"El taxi {taxi_id} no existe en la base de datos"}), 404

    except Exception as e:
        print(f"Error durante el registro del taxi: {e}")
        return jsonify({"error": f"Error al registrar taxi: {e}"}), 500

def leer_tokens():
    tokens = {}
    try:
        with open(ARCHIVO_TOKENS, "r") as archivo:
            for linea in archivo:
                taxi_id, token = linea.strip().split(":")
                tokens[taxi_id] = token
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Error al leer el archivo de tokens: {e}")
    return tokens

def borrar_taxi(taxi_id):
    taxis = leer_taxis()
    lista_taxis = []

    for taxi in taxis:
        if taxi['taxi_id'] == taxi_id:  # Cambia 'taxi' por 'taxi_id'
            lista_taxis.append(f"<NO><NO><{taxi['taxi_id']}><{taxi['pos_x']}><{taxi['pos_y']}>\n")
        else:
            lista_taxis.append(f"<{taxi['asignado']}><{taxi['operativo']}><{taxi['taxi_id']}><{taxi['pos_x']}><{taxi['pos_y']}>\n")

    with open(ARCHIVO_TAXI, "w") as archivo:
        for linea in lista_taxis:
            archivo.write(linea)

def borrarLineaClave(taxi_id):
    try:
        nuevas_lineas = []

        # Leer el archivo de tokens y filtrar las líneas
        with open(ARCHIVO_CLAVES, "r") as archivo:
            lineas = archivo.readlines()

        for linea in lineas:
            id_actual, _ = linea.strip().split(":")
            if id_actual != taxi_id:  # Mantener solo las líneas que no coincidan con el taxi_id
                nuevas_lineas.append(linea)

        # Escribir las líneas filtradas de vuelta en el archivo
        with open(ARCHIVO_CLAVES, "w") as archivo:
            archivo.writelines(nuevas_lineas)

        print(f"Taxi {taxi_id} eliminado correctamente del archivo de claves.")

    except FileNotFoundError:
        print(f"Error: El archivo {ARCHIVO_CLAVES} no existe.")
    except Exception as e:
        print(f"Error al borrar la línea del taxi {taxi_id}: {e}")

def borrarLineaTokens(taxi_id):
    try:
        nuevas_lineas = []

        # Leer el archivo de tokens y filtrar las líneas
        with open(ARCHIVO_TOKENS, "r") as archivo:
            lineas = archivo.readlines()

        for linea in lineas:
            id_actual, _ = linea.strip().split(":")
            if id_actual != taxi_id:  # Mantener solo las líneas que no coincidan con el taxi_id
                nuevas_lineas.append(linea)

        # Escribir las líneas filtradas de vuelta en el archivo
        with open(ARCHIVO_TOKENS, "w") as archivo:
            archivo.writelines(nuevas_lineas)

        print(f"Taxi {taxi_id} eliminado correctamente del archivo de tokens.")

    except FileNotFoundError:
        print(f"Error: El archivo {ARCHIVO_TOKENS} no existe.")
    except Exception as e:
        print(f"Error al borrar la línea del taxi {taxi_id}: {e}")
    
@app.route("/unregister_taxi", methods=["POST"])
def unregister_taxi():

    try:
        print("Solicitud recibida en /unregister_taxi")
        data = request.json
        print(f"Datos recibidos: {data}")
        taxi_id = data.get("taxi_id")
        print(f"Taxi ID: {taxi_id}")
        
        # Leer el archivo de taxis
        taxis = leer_taxis()
    
        taxi_encontrado = False
        for taxi in taxis:
            if taxi['taxi_id'] == taxi_id:
                taxi_encontrado = True

        if not taxi_encontrado:
            return jsonify({"error": f"El taxi {taxi_id} no existe en la base de datos"}), 404

        borrar_taxi(taxi_id)
        borrarLineaTokens(taxi_id)
        borrarLineaClave(taxi_id)

        return jsonify({"mensaje": f"Taxi {taxi_id} desconectado correctamente"}), 200

    except Exception as e:
        print(f"Error al desconectar taxi: {e}")
        return jsonify({"error": f"Error al desconectar taxi: {e}"}), 500

@app.route("/list_taxis", methods=["GET"])
def list_taxis():
    try:
        taxis = leer_taxis()
        return jsonify({"taxis": taxis}), 200
    except Exception as e:
        return jsonify({"error": f"Error al obtener la lista de taxis: {e}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)