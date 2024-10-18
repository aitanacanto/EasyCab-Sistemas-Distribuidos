import socket
import sys
import threading
import time
from kafka import KafkaConsumer, KafkaProducer
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
START = "START"

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

############# FUNCION CONSUMIDOR DE KAFKA #############



def recibirActualizacionMapa():
    # Configuración del consumer
    consumer = KafkaConsumer(
        'actualizar_mapa',                              # El tópico desde el que va a consumir
        bootstrap_servers='localhost:9092',             # Broker de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # Decodifica los mensajes recibidos
        auto_offset_reset='earliest',                   # Consume desde el principio si no hay offset
        group_id='taxi_group'                           # Agrupamos consumidores si hay varios taxis
    )

    # Ciclo para recibir y procesar los mensajes
    for message in consumer:
        mapa = message.value                            # Extraemos el mapa (array de strings)
        print(f"Mapa recibido de central: {mapa}")
        # Aquí puedes procesar el mapa como lo necesites dentro del DigitalEngine
        # Por ejemplo, actualizar el mapa local del taxi


def recibirPosicionCliente():
    # Configuración del consumer
    consumer = KafkaConsumer(
        'procesar_viaje',                              # El tópico desde el que va a consumir
        bootstrap_servers='localhost:9092',             # Broker de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # Decodifica los mensajes recibidos
        auto_offset_reset='earliest',                   # Consume desde el principio si no hay offset
    )

    return consumer.message


# Enviar actualizaciones del taxi
def actualizar_taxi_status(taxi_id, status):
    producerTAXI = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'actualizaciones_taxi'
    message = {'taxi_id': taxi_id, 'status': status}
    producerTAXI.send(topic, message)
    producerTAXI.flush()



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

            #mandarMensajeCentral()
            actualizar_taxi_status(ID_TAXI, )
    
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

def validarID():
    socketCentral.send(ID_TAXI.encode())            # Manda a central "START" para que este le mande si la ID solicitada está disponible
    respuesta = socketCentral.recv(4096)
    
    if respuesta.decode() == FIN:                   # El taxi tiene asignado la ID y puede ejercer
        socketCentral.close()
        exit()

    else:
        print("ID asignado al TAXI")

def conectarConServidor():
    global socketCentral
    socketCentral = socket.socket()
    socketCentral.connect((IP_CENTRAL, PUERTO_CENTRAL))
    print("CONECTADO AL SERVIDOR CENTRAL EN [" + IP_CENTRAL + " : " + str(PUERTO_CENTRAL) + "]")
    validarID(socketCentral)

############## MAIN ################

conectarConServidor()       # Conectamos el servidor con el DigitalEngine

# Creamos el hilo al Sensor y la ejecutamos
hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

hiloSensor.join()

############## MAIN ################

conectarConServidor()  # Servidor conectado con el DigitalEngine

# Hilo Sensor
hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

# Hilo para el consumidor de Kafka


# Esperar que el hilo del sensor termine (puedes cambiar esto según tu lógica)
hiloSensor.join()
# Puedes unirte al hilo del consumidor de Kafka si lo deseas
# hiloKafka.join()  # Descomenta si quieres esperar que el consumidor termine