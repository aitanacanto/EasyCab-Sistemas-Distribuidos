import socket
import sys
import threading
import time
from kafka import KafkaConsumer
import json

################ LO QUE FALTA ##############
# Falta por hacer el movimiento del coche






# Entradas: IP y Puerto de Central
#           IP y Puerto del gestor de colas
#           Ip y Puerto del Sensor
#           Id del taxi

if len(sys.argv) != 8:
    print("ERROR: Número de argumentos arróneos")    
    print("Formato correcto -> python3 EC_DE <IP central> <Puerto central> <IP gestor> <Puerto central> <IP Sensor> <Puerto Sensor> <ID Taxi>")
    exit()


argumentos = sys.argv

FIN = "FIN"

IP_CENTRAL = argumentos[1]
PUERTO_CENTRAL = int(argumentos[2])

IP_GESTOR = argumentos[3]
PUERTO_GESTOR = int(argumentos[4])

IP_SENSOR = argumentos[5]
PUERTO_SENSOR = int(argumentos[6])
ID_TAXI = argumentos[7]

############ FUNCIONES #############
 
def conexionSensor():
    socketSensor = socket.socket()
    socketSensor.bind((IP_SENSOR, PUERTO_SENSOR))
    socketSensor.listen(60)

    print("SERVIDOR CREADO [" + IP_SENSOR + " : " + str(PUERTO_SENSOR) + "]")
    conexion, addr = socketSensor.accept()

    print("Conexión hecha con Sensor en " + str(addr))

    while True:
        try:
            mensaje = conexion.recv(4096)
            if mensaje == FIN:
                break

            print("Mensaje Sensor entrante: [" + mensaje.decode() + "]")

            mandarMensajeCentral()
    
        except Exception as e:
            break

    conexion.close()
    socketSensor.close()

def mandarMensajeCentral():
    global socketCentral
    # Aquí se hace todo lo que se tiene que hacer con el mapa
    mapa = "MAPA"
    socketCentral.send(mapa.encode())

    respuesta = socketCentral.recv(4096)
    print("Recibido de la central: " + respuesta.decode())

    if respuesta.decode() == FIN:                                                   # Acabar la conexión cuando el servidor mande un FIN
        print("CONEXIÓN CERRADA CON EL SERVIDOR CENTRAL")

def conectarConServidor():
    global socketCentral
    socketCentral = socket.socket()
    socketCentral.connect((IP_CENTRAL, PUERTO_CENTRAL))
    print("CONECTADO AL SERVIDOR CENTRAL EN [" + IP_CENTRAL + " : " + str(PUERTO_CENTRAL) + "]")

############## MAIN ################

conectarConServidor()       # Conectamos el servidor con el DigitalEngine

# Creamos el hilo al Sensor y la ejecutamos
hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

hiloSensor.join()

############# FUNCION CONSUMIDOR DE KAFKA #############

def consumidorKafka():
    consumer = KafkaConsumer(
        'asignaciones_taxi',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='grupo_taxis',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Esperando asignaciones de taxis...")
    for message in consumer:
        asignacion = message.value
        print(f"Asignación recibida: Taxi asignado a {asignacion['cliente_id']} desde {asignacion['ubicacion']} a {asignacion['destino']}")
        # Aquí puedes implementar la lógica para mover el taxi o responder a la asignación.

############## MAIN ################

conectarConServidor()  # Servidor conectado con el DigitalEngine

# Hilo Sensor
hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

# Hilo para el consumidor de Kafka
hiloKafka = threading.Thread(target=consumidorKafka)
hiloKafka.start()

# Esperar que el hilo del sensor termine (puedes cambiar esto según tu lógica)
hiloSensor.join()
# Puedes unirte al hilo del consumidor de Kafka si lo deseas
# hiloKafka.join()  # Descomenta si quieres esperar que el consumidor termine