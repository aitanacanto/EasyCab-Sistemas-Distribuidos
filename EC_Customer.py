import socket
import sys

# Entradas: IP y Puerto del gestor de colas
#           Id del cliente

argumentos = sys.argv

if len(sys.argv) != 4:
    print("ERROR: Número de argumentos arróneos")
    print("Formato correcto -> ./ejecutable <IP gestor> <> <>")
    exit()
    