import socket
import sys
import threading
import time

# Entradas: IP y Puerto de Central
#           IP y Puerto del gestor de colas
#           Ip y Puerto del Sensor
#           Id del taxi

if len(sys.argv) != 8:
    print("ERROR: Número de argumentos arróneos")    
    print("Formato correcto -> ./ejecutable <IP central> <Puerto central> <IP gestor> <Puerto central> <IP Sensor> <Puerto Sensor>")
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
    socketSensor.listen(1)

    print("SERVIDOR CREADO [" + IP_SENSOR + " : " + PUERTO_SENSOR + "]")
    conexion, addr = socketSensor.accept()

    print("Conexión hecha con Sensor en " + addr)

    while True:
        mensaje = conexion.recv(4096)
        if not mensaje:
            break
        print("Mensaje Sensor entrante: [" + mensaje.decode() + "]")

    conexion.close()
    socketSensor.close()

def conexionCentral():
    socketCentral = socket.socket()
    socketCentral.connect((IP_CENTRAL, PUERTO_CENTRAL))
    print("CONECTADO AL SERVIDOR CENTRAL EN [" + IP_CENTRAL + " : " + PUERTO_CENTRAL + "]")

    while True:
        # Aquí se hace todo lo que se tiene que hacer con el mapa
        mapa = "MAPAAAAAAAAAAAAAAAAAAA"
        socketCentral.send(mapa.encode())

        respuesta = socketCentral.recv(4096)
        print("Recibido de la central: " + respuesta.decode())

        if respuesta.decode() == FIN:
            print("CONEXIÓNO CERRADA CON EL SERVIDOR CENTRAL")
            break

    socketCentral.close()

############## MAIN ################

# Creamos el hilo al Sensor y la ejecutamos
hiloSensor = threading.Thread(target=conexionSensor)
hiloSensor.start()

# Creamos el hilo a la Central y la ejecutamos
hiloCentral = threading.Thread(target=conexionCentral)
hiloCentral.start()

# Esperamos a que los hilos acaben
hiloSensor.join()
hiloCentral.join()


