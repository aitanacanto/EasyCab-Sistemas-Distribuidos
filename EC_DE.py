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
    print("Formato correcto -> python3 EC_DE <IP central> <Puerto central> <IP gestor> <Puerto gestor> <IP Sensor> <Puerto Sensor> <ID Taxi>")
    exit()

argumentos = sys.argv

FIN = "<FIN>"
START = "START"
OK = "OK"
KO = "KO"

ALTO = 20
ANCHO = 20

IP_CENTRAL = argumentos[1]
PUERTO_CENTRAL = int(argumentos[2])

IP_GESTOR = argumentos[3]
PUERTO_GESTOR = int(argumentos[4])

IP_SENSOR = argumentos[5]
PUERTO_SENSOR = int(argumentos[6])

ID_TAXI = argumentos[7]

# Esto es la ubicación del taxi en el mapa
global ubicacion

# Esto es un array, [0] -> posClienteX, [1] -> posClienteY
global cliente

# Esto es un arrat, [0] -> posDestinoX, [1] -> posDestinoY
global destino

# Booleano, dice si el cliente está ya en el taxi (True) o está yendo hacia él (False)
global clienteDentro

# Esto es un array de String, en resumen, es el mapa actualizado
global mapa

# Booleano, dice si el taxi puede moverse (OK) o no (KO)
global permisoMovimiento

############ FUNCIONES #############

# Cambia la variable global mapa
def recibirActualizacionMapa():
    global mapa
    # Configuración del consumer
    consumer = KafkaConsumer(
        'actualizar_mapa',                              # El tópico desde el que va a consumir
        bootstrap_servers='localhost:9092',             # Broker de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # Decodifica los mensajes recibidos
        auto_offset_reset='latest',                     # Consume desde el principio si no hay offset
    )

    # Ciclo para recibir y procesar los mensajes
    for message in consumer:
        mapa = message.value                            # Extraemos el mapa (array de strings)
    return

# Cambia la variable global cliente
def recibirPosicionCliente():
    global cliente
    global destino
    # Configuración del consumidor
    consumer = KafkaConsumer(
        'procesar_viaje',  # El tópico desde el que va a consumir
        bootstrap_servers='localhost:9092',  # Broker de Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # Deserializa los mensajes recibidos
        auto_offset_reset='earliest',  # Consume desde el principio si no hay offset
        enable_auto_commit=False,  # Desactivar el autocommit para que no se marquen automáticamente como leídos
        group_id='taxi_group'  # Grupo de consumidores
    )

    # Bucle para leer mensajes
    for message in consumer:
        info = message.value
        taxi_id = info['taxi_id']

        # Si el mensaje es para el ID_TAXI específico
        if taxi_id == ID_TAXI:
            cliente = info['posCliente']
            destino = info['posDestino']
            # Realizar el commit solo para el mensaje que se ha procesado
            consumer.commit()
    return None

# Enviar actualizaciones del taxi
def enviarPosicionTaxi(taxi_id, posicion, finalizado):
    producerTAXI = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'actualizaciones_taxi'
    message = {'taxi_id': taxi_id, 'taxi_posicion': posicion, 'viaje_finalizado': finalizado}
    producerTAXI.send(topic, message)
    producerTAXI.flush()

    return 

def mostrarMapa(mapa):
    for y in range(ALTO):
        fila = []
        for x in range(ANCHO): 
            fila.append(mapa[x][y])

        print(' '.join(fila))

    return

def conexionSensor():
    global permisoMovimiento
    socketSensor = socket.socket()
    socketSensor.bind((IP_SENSOR, PUERTO_SENSOR))
    socketSensor.listen(60)

    print("SERVIDOR CREADO [" + IP_SENSOR + " : " + str(PUERTO_SENSOR) + "]")
    conexion, addr = socketSensor.accept()

    print("Conexión hecha con Sensor en " + str(addr))

    while True:
        try:
            mensaje = conexion.recv(4096)
            if mensaje == FIN:   break
            elif mensaje == OK:  permisoMovimiento = True
            elif mensaje == KO:  permisoMovimiento = False

            print("Mensaje Sensor entrante: [" + mensaje.decode() + "]")
    
        except Exception as e:
            break

    conexion.close()
    socketSensor.close()
    
    return 

def movimiento(posX, posY, objetivo, movimiento):
    if movimiento == "Vertical":
        ubicacion = posY
        destino = objetivo[1]
    else:
        ubicacion = posX
        destino = objetivo[0]
    
    posNueva = 0

    distanciaDirecta = abs(ubicacion - destino)
    distanciaPorFuera = ALTO - distanciaDirecta

    if distanciaDirecta < distanciaPorFuera:
        if ubicacion < destino:
            if ubicacion == ANCHO-1:
                posNueva = 0
            else:
                posNueva = ubicacion+1
        else:
            if ubicacion == 0:
                posNueva = ANCHO-1
            else:
                posNueva = ubicacion-1
    else:
        if ubicacion < destino:
            if ubicacion == ANCHO-1:
                posNueva = 0
            else:
                posNueva = ubicacion-1
        else:
            if ubicacion == ANCHO-1:
                posNueva = 0
            else:
                posNueva = ubicacion+1

    if movimiento == "Vertical":
        return (posX, posNueva)
    else:
        return (posNueva, posY)

def actualizarPosicion(cliente, destino, mapa):
    global ubicacion
    global clienteDentro

    posX = ubicacion[0]
    posY = ubicacion[1]

    if clienteDentro:
        objetivo = destino
    else:
        objetivo = cliente

    if posX == objetivo[0]:
        return movimiento(posX, posY, objetivo, "Vertical")
    else:
        return movimiento(posX, posY, objetivo, "Horizontal")

def validarID(socketCentral):
    global ubicacion
    socketCentral.send(ID_TAXI.encode())            # Manda a central "START" para que este le mande si la ID solicitada está disponible
    respuesta = socketCentral.recv(4096)
    
    datos = respuesta.decode().strip().replace('<', '').split('>')

    if respuesta.decode() == FIN:                   # El taxi tiene asignado la ID y puede ejercer
        print("ERROR: ID del taxi se está utilizando")
        socketCentral.close()
        exit()
    else:
        ubicacion = (datos[1], datos[2])
        print("ID asignado con éxito")
    
    return 

def conectarConServidor():
    global socketCentral
    socketCentral = socket.socket()
    print("CONECTADO AL SERVIDOR CENTRAL EN [" + IP_CENTRAL + " : " + str(PUERTO_CENTRAL) + "]")
    socketCentral.connect((IP_CENTRAL, PUERTO_CENTRAL))
    validarID(socketCentral)
    return 

############## MAIN ################

conectarConServidor()  # Servidor conectado con el DigitalEngine

# Hilo Sensor
hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()


hiloSensor.join()
# Puedes unirte al hilo del consumidor de Kafka si lo deseas
# hiloKafka.join()  # Descomenta si quieres esperar que el consumidor termine