import socket
import sys

# Entradas: IP y Puerto de Central
#           IP y Puerto del gestor de colas
#           Ip y Puerto del Sensor
#           Id del taxi

if len(sys.argv) != 8:
    print("ERROR : Numero de argumentos erroneo")
    exit()

argumentos = sys.argv

IP_CENTRAL = argumentos[1]
PUERTO_CENTRAL = argumentos[2]
IP_GESTOR = argumentos[3]
PUERTO_GESTOR = argumentos[4]
IP_SENSOR = argumentos[5]
PUERTO_SENSOR = argumentos[6]
ID_TAXI = argumentos[7]

############ FUNCIONES #############
 




############## MAIN ################



