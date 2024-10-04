import socket
import sys

# Entradas: IP y Puerto del DigitalEngine

if len(sys.argv) != 3:
    print("ERROR: Número de argumentos arróneos")    
    print("Formato correcto -> ./ejecutable <IP DigitalEngine> <Puerto DigitalEngine>")
    exit()

argumentos = sys.argv

FIN = "FIN"

IP_DE = argumentos[1]
PUERTO_DE = int(argumentos[2])


############ FUNCIONES #############



############## MAIN ################

