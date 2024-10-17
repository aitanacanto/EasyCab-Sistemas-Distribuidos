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
            fila.append(mapa[x][y])

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

def actualizarPosicion(cliente, destino, objetivo):                 # Objetivo puede ser "C" o "D" dependiendo si el taxi va a recoger al cliente o va al destino
    if objetivo == "C":
        final = cliente
    else:
        final = destino

    caminoMinimo = " "

    if cliente[0] != final[0]:                                      # Aquí nos movemos en horizontal
        posInicial = cliente[0]
        posFinal = final[0]
        movimiento = "HORIZONTAL"
    else:
        posInicial = cliente[1]
        posFinal = final[1]
        movimiento = "VERTICAL"


    caminoPorDentro = abs(posInicial - posFinal)                # Movimiento si va por dentro del mapa
    caminoPorFuera = ANCHO - caminoPorDentro                    # Movimiento si va por fuera del mapa, pj sale de la derecha y entra por la izquierda

    if caminoPorFuera < caminoPorDentro:
        caminoMinimo = "FUERA"
    else:
        caminoMinimo = "DENTRO"


    if movimiento == "HORIZONTAL":
        if (posInicial < posFinal and caminoMinimo == "DENTRO") or (posInicial > posFinal and caminoMinimo == "FUERA"):
            posicionFinal = (cliente[0]+1, cliente[1])              # Se mueve a la derecha
        else:
            posicionFinal = (cliente[0]-1, cliente[1])              # Se mueve a la izquierda
    
    else:
        if (posInicial < posFinal and caminoMinimo == "DENTRO") or (posInicial > posFinal and caminoMinimo == "FUERA"):
            posicionFinal = (cliente[0], cliente[1]+1)              # Se mueve a la derecha
        else:
            posicionFinal = (cliente[0], cliente[1]-1)              # Se mueve a la izquierda

    return posicionFinal

def mandarMensajeCentral():

    try:
        # Recibir respuesta de la central
        # @KAFKA -> Recibe el mapa del servidor

        # @descomentar
        """
        if not mapa:                                    # Si no se recibe nada, cerrar conexión
            print("No se recibió respuesta. Cerrando conexión.")
            conn.close()
            return

        # Implementa la actualización del taxi
        if mapa.decode('utf-8') == FIN:                 # Acabar la conexión cuando el servidor mande un FIN
            print("CONEXIÓN CERRADA CON LA CENTRAL")
            return 

        mostrarMapa(mapa)                               # Recibo el mapa desde la central

        """
        # Actualizamos el mapa (moviendo al taxi hasta su objetivo)

        cliente = (12,8)
        destino = (5,5)                                  # @Implementar -> Pasar el destino del taxi (puede ser el cliente o el destino)
        objetivo = 'C'                                   # Objetivo = 'c': El destino del taxi es el cliente / 'D': El destino del taxi es el final
        actualizarPosicion(cliente, destino, objetivo)

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