from flask import Flask, jsonify
import threading
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Habilitar CORS para todas las rutas


# Estado del mapa
global mapa
mapa = [[" " for _ in range(20)] for _ in range(20)]

lock = threading.Lock()

# Endpoint para obtener el estado actual del mapa
@app.route('/mapa', methods=['GET'])
def obtener_mapa():
    global mapa
    with lock:
        return jsonify({"mapa": mapa})

# Función para iniciar la API en un hilo
def iniciar_api():
    app.run(host="0.0.0.0", port=5002, debug=False)

# Función para actualizar el mapa desde EC_Central
def actualizar_mapa(nuevo_mapa):
    global mapa
    with lock:
        mapa = nuevo_mapa
