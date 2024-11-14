# Levan Kipiani, Aitana Cantó
import tkinter as tk
from tkinter import messagebox
from kafka import KafkaProducer, KafkaConsumer, OffsetAndMetadata
import json
import time
import sys
import traceback
import threading
import random


global numeroIdentificacion
global id_customer
id_customer = ''
ANCHO = 20
ALTO = 20

# Función para procesar argumentos
if len(sys.argv) != 3:
    print("ERROR: Número de argumentos incorrecto")    
    print("Formato correcto -> python3 EC_Customer.py <IP_GESTOR> <op>\n")
    print("OP: <-f> -> Formulario / <nombre.json> -> Fichero destinos")
    exit()

argumentos = sys.argv
IP_GESTOR = str(argumentos[1])
OPCION = str(argumentos[2])
global ubicacion_taxi

# Función para serializar el mensaje a JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Inicializar productor de Kafka
producer = KafkaProducer(bootstrap_servers=[f"{IP_GESTOR}:{'9092'}"],
                         value_serializer=json_serializer)

def recibirMensajesDeCentral():
    global id_customer
    global numeroIdentificacion
    global ubicacion_taxi

    consumer = KafkaConsumer(
        'mensajes_a_consumer',
        bootstrap_servers=[f"{IP_GESTOR}:{'9092'}"],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='customer_group'
    )

    try:
        messages = consumer.poll(timeout_ms=100)

        if messages:
            for topic_partition, msgs in messages.items():
                for message in msgs:
                    info = message.value
                    codigoMensaje = info['codigoMensaje']

                    if codigoMensaje == '1':
                        if numeroIdentificacion == str(info['mensaje']):
                            id_customer = str(info['customerID'])
                            print("ID asignado al Customer")
                            consumer.commit({topic_partition: OffsetAndMetadata(message.offset + 1, None)})

                    elif id_customer:
                        if codigoMensaje == '2':
                            if id_customer == str(info['customerID']):
                                consumer.commit({topic_partition: OffsetAndMetadata(message.offset + 1, None)})
                                
                                if str(info['mensaje']) == "RECOGER":
                                    print("El taxi está en camino")
                                    messagebox.showinfo("Solicitud procesada","El taxi está en camino.")
                                    return False

                                elif str(info['mensaje']) == "FINALIZADO":
                                    print("Aparcao, hasta la proxima")
                                    messagebox.showinfo("Éxito", "El taxi ha llegado a su destino.")
                                    ubicacion_taxi = info['ubicacion_taxi']
                                    return True

    except Exception as e:
        print(f"Error al recibir la orden del servicio: {e}")
        traceback.print_exc()
    return False

def mensajeriaConCentral():
    while True:
        recibirMensajesDeCentral()


# Función para enviar la solicitud de taxi
def enviar_solicitud_taxi(cliente_nombre, destino):
    global numeroIdentificacion
    numero_aleatorio = str(random.randint(1, 10000))
    numeroIdentificacion = numero_aleatorio

    solicitud = {
        "numero_identificacion": numero_aleatorio,
        "destinos" : destino,
    }
    
    producer.send('solicitud_taxi', solicitud)
    print(f"Solicitud enviada, en breves su taxi pasara a por usted,", cliente_nombre,".")
    producer.flush()




def obtener_lista_destinos(ruta_archivo):
    try:
        with open(ruta_archivo, 'r') as archivo:
            datos = json.load(archivo)  # Cargar el JSON
            lista_destinos = [request["Id"] for request in datos["Requests"]]
        return lista_destinos
    
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        messagebox.showerror("Error", "No se pudo leer el archivo pasado por parámetro")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        messagebox.showerror("Error", "No se pudo leer el archivo pasado por parámetro")

    ventana.destroy()
    return []

    
# Función para procesar los datos del formulario
def procesar_formulario(lista_destinos):
    cliente_nombre = etiqueta_cliente_nombre.get()
    ubicacion_x = entrada_ubicacion_x.get()
    ubicacion_y = entrada_ubicacion_y.get()

    if not cliente_nombre or not ubicacion_x or not ubicacion_y:
        messagebox.showerror("Error", "Por favor, complete todos los campos.")
        return
    
    ubicacion = (int(ubicacion_x), int(ubicacion_y))

    if OPCION == "-f":
        destino_x = entrada_destino_x.get()
        destino_y = entrada_destino_y.get()

        if not destino_x or not destino_y:
            messagebox.showerror("Error", "Por favor, complete todos los campos.")
            return
        
        destino = (int(destino_x), int(destino_y))

        if (destino[0] or ubicacion[0]) > ANCHO or (destino[1] or ubicacion[1]) > ALTO:
            messagebox.showerror("Error", "Por favor, no todos los campos son correctos.")
            return

        destinos = [ubicacion, destino]

        enviar_solicitud_taxi(cliente_nombre, destinos)

    else:
        if ubicacion[0] > ANCHO or ubicacion[1] > ALTO:
            messagebox.showerror("Error", "Por favor, no todos los campos son correctos.")
            return
        
        
        lista_destinos.insert(0, {"Id": id_customer, "POS": (int(ubicacion[0]-1),int(ubicacion[1]-1))})
        
        enviar_solicitud_taxi(cliente_nombre, lista_destinos)
    
    # Mostrar mensaje de éxito
    messagebox.showinfo("Éxito", "Solicitud de taxi enviada con éxito!")

    hiloRecibirMensajes = threading.Thread(target=mensajeriaConCentral)
    hiloRecibirMensajes.start()
    hiloRecibirMensajes.join()

# Crear la ventana principal
ventana = tk.Tk()
ventana.title("Formulario de Solicitud de Taxi")

# Etiquetas y campos de entrada para el cliente ID
etiqueta_cliente_nombre = tk.Label(ventana, text="Nombre del Cliente:")
etiqueta_cliente_nombre.grid(row=0, column=0)
etiqueta_cliente_nombre = tk.Entry(ventana)
etiqueta_cliente_nombre.grid(row=0, column=1)

# Etiquetas y campos de entrada para la ubicación (x, y)
etiqueta_ubicacion = tk.Label(ventana, text="Ubicación (x, y):")
etiqueta_ubicacion.grid(row=1, column=0)
entrada_ubicacion_x = tk.Entry(ventana, width=5)
entrada_ubicacion_x.grid(row=1, column=1)
entrada_ubicacion_y = tk.Entry(ventana, width=5)
entrada_ubicacion_y.grid(row=1, column=2)

if OPCION == "-f" :

    # Etiquetas y campos de entrada para el destino (x, y)
    etiqueta_destino = tk.Label(ventana, text="Destino (x, y):")
    etiqueta_destino.grid(row=2, column=0)
    entrada_destino_x = tk.Entry(ventana, width=5)
    entrada_destino_x.grid(row=2, column=1)
    entrada_destino_y = tk.Entry(ventana, width=5)
    entrada_destino_y.grid(row=2, column=2)

lista_destinos = []

if OPCION != "-f" :
    lista_destinos = obtener_lista_destinos(OPCION)

# Botón para enviar la solicitud
boton_enviar = tk.Button(ventana, text="Enviar Solicitud", command=lambda: procesar_formulario(lista_destinos))
boton_enviar.grid(row=3, columnspan=3)




# Ejecutar la ventana principal
ventana.mainloop()