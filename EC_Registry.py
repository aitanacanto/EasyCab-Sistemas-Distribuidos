from flask import Flask, request, jsonify
import os

app = Flask(__name__)

# Archivo existente de taxis
ARCHIVO_TAXI = "taxi.txt"

# Crear el archivo de taxis si no existe
if not os.path.exists(ARCHIVO_TAXI):
    with open(ARCHIVO_TAXI, "w") as archivo:
        archivo.write("")  # Crear un archivo vacío


# Leer el archivo de taxis
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
    except Exception as e:
        print(f"Error al leer el archivo de taxis: {e}")
    return taxis


# Guardar en el archivo de taxis
def guardar_taxis(taxis):
    try:
        with open(ARCHIVO_TAXI, "w") as archivo:
            for taxi in taxis:
                archivo.write(f"<{taxi['asignado']}><{taxi['operativo']}><{taxi['taxi_id']}><{taxi['pos_x']}><{taxi['pos_y']}>\n")
    except Exception as e:
        print(f"Error al guardar en el archivo de taxis: {e}")


@app.route("/register_taxi", methods=["POST"])
def register_taxi():
    try:
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
                return jsonify({"error": f"El taxi {taxi_id} ya está registrado"}), 400

        # Agregar el nuevo taxi
        nuevo_taxi = {"asignado": "NO", "operativo": "NO", "taxi_id": taxi_id, "pos_x": pos_x, "pos_y": pos_y}
        taxis.append(nuevo_taxi)
        guardar_taxis(taxis)

        return jsonify({"mensaje": f"Taxi {taxi_id} registrado con éxito", "taxi": nuevo_taxi}), 201

    except Exception as e:
        return jsonify({"error": f"Error al registrar taxi: {e}"}), 500


@app.route("/unregister_taxi/<taxi_id>", methods=["DELETE"])
def unregister_taxi(taxi_id):
    try:
        # Leer el archivo de taxis
        taxis = leer_taxis()

        # Filtrar el taxi que debe ser eliminado
        taxis_actualizados = [taxi for taxi in taxis if taxi["taxi_id"] != taxi_id]

        if len(taxis) == len(taxis_actualizados):
            return jsonify({"error": f"Taxi {taxi_id} no encontrado"}), 404

        # Guardar los cambios
        guardar_taxis(taxis_actualizados)

        return jsonify({"mensaje": f"Taxi {taxi_id} dado de baja con éxito"}), 200

    except Exception as e:
        return jsonify({"error": f"Error al dar de baja taxi: {e}"}), 500


@app.route("/list_taxis", methods=["GET"])
def list_taxis():
    try:
        taxis = leer_taxis()
        return jsonify({"taxis": taxis}), 200
    except Exception as e:
        return jsonify({"error": f"Error al obtener la lista de taxis: {e}"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
