from flask import Flask, jsonify
import threading
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Habilitar CORS para todas las rutas

# Estado del mapa y taxis
global mapa, estados_taxis
mapa = [[" " for _ in range(20)] for _ in range(20)]
estados_taxis = []

lock = threading.Lock()

# Endpoint para obtener el estado actual del mapa y taxis
@app.route('/mapa', methods=['GET'])
def obtener_mapa():
    global mapa, estados_taxis
    with lock:
        return jsonify({"mapa": mapa, "estados_taxis": estados_taxis})

# Función para iniciar la API en un hilo
def iniciar_api():
    app.run(host="0.0.0.0", port=5003, debug=False)

# Función para actualizar el mapa y estado de los taxis desde EC_Central
def actualizar_mapa(nuevo_mapa, nuevo_estado_taxis):
    global mapa, estados_taxis
    with lock:
        mapa = nuevo_mapa
        estados_taxis = nuevo_estado_taxis
