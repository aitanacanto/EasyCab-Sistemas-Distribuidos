import socket
import sys
import threading
import time
from kafka import KafkaConsumer
import json

################ LO QUE FALTA ##############
# Falta hacer el movimiento del taxi hasta el cliente y luego hasta el destino

####### PREGUNTAS #######

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
ALTO = 20
ANCHO = 20
NADA = 0x00

IP_CENTRAL = argumentos[1]
PUERTO_CENTRAL = int(argumentos[2])

IP_GESTOR = argumentos[3]
PUERTO_GESTOR = int(argumentos[4])

IP_SENSOR = argumentos[5]
PUERTO_SENSOR = int(argumentos[6])
ID_TAXI = argumentos[7]

############ FUNCIONES #############

def mostrarMapa(mapa):
    for y in range(ALTO):
        fila = []
        for x in range(ANCHO): 
            index = y * ANCHO + x
            valor = mapa[index]

            if valor == NADA:
                fila.append('.')                    # Celda vacía
            elif 32 <= valor <= 126:                # De 32 a 126 es el rango para representar el ASSCI
                fila.append(chr(valor))             # Representa el byte como char
            elif (valor & 0x3F) != 0:               # Si el taxi está parado
                fila.append(f"{valor & 0x3F}")     # Solo muestra el ID del taxi
            else:
                fila.append('?')                    # Elemento desconocido

        print(' '.join(fila))

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

def actualizarMapa(destino, objetivo):
    print("")

def mandarMensajeCentral():
    global socketCentral

    try:
        # Recibir respuesta de la central
        # @KAFKA -> Recibe el mapa del servidor

        # @descomentar
        """
        if not mapa:                                    # Si no se recibe nada, cerrar conexión
            print("No se recibió respuesta. Cerrando conexión.")
            return

        # Implementa la actualización del taxi
        if mapa.decode('utf-8') == FIN:                 # Acabar la conexión cuando el servidor mande un FIN
            print("CONEXIÓN CERRADA CON LA CENTRAL")
            return 

        mostrarMapa(mapa)                               # Recibo el mapa desde la central

        """
        # Actualizamos el mapa (moviendo al taxi hasta su objetivo)
        # ¿Enviamos solo la posición nueva del taxi o enviamos el mapa entero?

        destino = (5,5)                                 # @Implementar -> Pasar el destino del taxi (puede ser el cliente o el destino)
        objetivo = 'c'                                  # Objetivo = 'c': El destino del taxi es el cliente / 'f': El destino del taxi es el final
        actualizarMapa(destino, objetivo)

        # @KAFKA -> Envía el mapa actualizado al servidor

    except Exception as e:
        print(f"Error en la comunicación con la central: {e}")
        return 


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