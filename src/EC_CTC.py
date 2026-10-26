from flask import Flask, jsonify                    # type: ignore
import requests
import threading
import time
import tkinter as tk
from tkinter import messagebox
import sys

app = Flask(__name__)

# Variables compartidas
ciudad_actual = "Benidorm"
estado_trafico = "OK"

if len(sys.argv) != 2:
    print("ERROR: Número de argumentos incorrecto")
    print("Formato correcto -> python3 src/EC_CTC.py <archivo_APIKEY>")
    sys.exit(1)

argumentos = sys.argv
ARCHIVO_APIKEY = argumentos[1]

# API Key y URL base de OpenWeather
#API_KEY = "example"

with open(ARCHIVO_APIKEY, "r") as archivo:
    API_KEY = archivo.readline().strip()

        
URL_BASE = "http://api.openweathermap.org/data/2.5/weather"

# Función para actualizar el estado del tráfico periódicamente
def actualizar_estado_trafico():
    global estado_trafico, ciudad_actual
    while True:
        try:
            # Llamada a OpenWeather
            url = f"{URL_BASE}?q={ciudad_actual}&appid={API_KEY}&units=metric"
            respuesta = requests.get(url)
            datos = respuesta.json()

            # Determinar el estado del tráfico según la temperatura
            temperatura = datos['main']['temp']
            estado_trafico = "OK" if temperatura > 0 else "KO"
        except Exception as e:
            print(f"Error al obtener el clima: {e}")
            estado_trafico = "KO"

        time.sleep(3)

# Ruta para consultar el estado del tráfico
@app.route("/trafico", methods=["GET"])
def obtener_estado_trafico():
    global estado_trafico
    return jsonify({"ciudad": ciudad_actual, "estado_trafico": estado_trafico})

# Interfaz Gráfica para Cambiar de Ciudad
def iniciar_interfaz():
    def cambiar_ciudad():
        global ciudad_actual
        nueva_ciudad = entrada_ciudad.get()
        if nueva_ciudad:
            ciudad_actual = nueva_ciudad
            messagebox.showinfo("Éxito", f"Ciudad cambiada a: {ciudad_actual}")
        else:
            messagebox.showerror("Error", "Por favor, introduce una ciudad válida.")

    # Crear ventana
    ventana = tk.Tk()
    ventana.title("EC_CTC - Cambiar Ciudad")
    ventana.geometry("300x150")

    # Etiqueta y entrada para la ciudad
    etiqueta = tk.Label(ventana, text="Introduce una nueva ciudad:", font=("Helvetica", 12))
    etiqueta.pack(pady=10)
    entrada_ciudad = tk.Entry(ventana, width=25, font=("Helvetica", 12))
    entrada_ciudad.pack(pady=5)

    # Botón para cambiar la ciudad
    boton = tk.Button(ventana, text="Cambiar Ciudad", command=cambiar_ciudad, font=("Helvetica", 12))
    boton.pack(pady=10)

    # Iniciar el bucle de la ventana
    ventana.mainloop()

# Hilo para iniciar Flask
def iniciar_flask():
    app.run(host="0.0.0.0", port=5000)

# Ejecutar ambos componentes en paralelo
if __name__ == "__main__":
    # Hilo para actualizar el estado del tráfico
    hilo_actualizacion = threading.Thread(target=actualizar_estado_trafico, daemon=True)
    hilo_actualizacion.start()

    # Hilo para ejecutar Flask
    hilo_flask = threading.Thread(target=iniciar_flask, daemon=True)
    hilo_flask.start()

    # Iniciar la interfaz gráfica
    iniciar_interfaz()