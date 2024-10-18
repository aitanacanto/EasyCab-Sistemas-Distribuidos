import tkinter as tk
from tkinter import messagebox
from kafka import KafkaProducer
import json

# Función para serializar el mensaje a JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Inicializar productor de Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

# Función para enviar la solicitud de taxi
def enviar_solicitud_taxi(cliente_id, ubicacion, destino):
    solicitud = {
        "cliente_id": cliente_id,
        "ubicacion": ubicacion,
        "destino": destino
    }
    
    producer.send('solicitud_taxi', solicitud)
    print(f"Solicitud enviada")
    producer.flush()
    
# Función para procesar los datos del formulario
def procesar_formulario():
    cliente_id = entrada_cliente_id.get()
    ubicacion_x = entrada_ubicacion_x.get()
    ubicacion_y = entrada_ubicacion_y.get()
    destino_x = entrada_destino_x.get()
    destino_y = entrada_destino_y.get()
    
    # Convertir ubicación y destino en tuplas (x, y)
    ubicacion = (int(ubicacion_x), int(ubicacion_y))
    destino = (int(destino_x), int(destino_y))
    
    # Validar que los campos no estén vacíos
    if not cliente_id or not ubicacion_x or not ubicacion_y or not destino_x or not destino_y:
        messagebox.showerror("Error", "Por favor, complete todos los campos.")
        return
    
    # Llamar a la función enviar_solicitud_taxi con los datos
    enviar_solicitud_taxi(cliente_id, ubicacion, destino)
    
    # Mostrar mensaje de éxito
    messagebox.showinfo("Éxito", "Solicitud de taxi enviada con éxito!")

# Crear la ventana principal
ventana = tk.Tk()
ventana.title("Formulario de Solicitud de Taxi")

# Etiquetas y campos de entrada para el cliente ID
etiqueta_cliente_id = tk.Label(ventana, text="ID del Cliente:")
etiqueta_cliente_id.grid(row=0, column=0)
entrada_cliente_id = tk.Entry(ventana)
entrada_cliente_id.grid(row=0, column=1)

# Etiquetas y campos de entrada para la ubicación (x, y)
etiqueta_ubicacion = tk.Label(ventana, text="Ubicación (x, y):")
etiqueta_ubicacion.grid(row=1, column=0)
entrada_ubicacion_x = tk.Entry(ventana, width=5)
entrada_ubicacion_x.grid(row=1, column=1)
entrada_ubicacion_y = tk.Entry(ventana, width=5)
entrada_ubicacion_y.grid(row=1, column=2)

# Etiquetas y campos de entrada para el destino (x, y)
etiqueta_destino = tk.Label(ventana, text="Destino (x, y):")
etiqueta_destino.grid(row=2, column=0)
entrada_destino_x = tk.Entry(ventana, width=5)
entrada_destino_x.grid(row=2, column=1)
entrada_destino_y = tk.Entry(ventana, width=5)
entrada_destino_y.grid(row=2, column=2)

# Botón para enviar la solicitud
boton_enviar = tk.Button(ventana, text="Enviar Solicitud", command=procesar_formulario)
boton_enviar.grid(row=3, columnspan=3)

# Ejecutar la ventana principal
ventana.mainloop()
