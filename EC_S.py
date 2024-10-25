import socket
import sys
import tkinter as tk
import threading
import time

# Entradas: IP y Puerto del DigitalEngine

if len(sys.argv) != 3:
    print("ERROR: Número de argumentos arróneos")    
    print("Formato correcto -> python3 EC_S.py <IP DigitalEngine> <Puerto DigitalEngine>")
    exit()

argumentos = sys.argv

FIN = "<FIN>"
OK = "OK"
KO = "KO"

IP_DE = argumentos[1]
PUERTO_DE = int(argumentos[2])

############ FUNCIONES #############

def conectarDigitalEngine():
    global socket_de
    socket_de = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_de.connect((IP_DE, PUERTO_DE))
    print("CONECTADO AL SERVIDOR CENTRAL EN [" + IP_DE + " : " + str(PUERTO_DE) + "]")

def enviarMensajes():
    global stop
    stop = False

    while not stop:  # Mientras no se active el evento de parada
        try:
            socket_de.send(OK.encode('utf-8'))
            print("Enviado: OK")
            time.sleep(1)  # Esperar 1 segundo antes de enviar el siguiente OK
        
        except Exception as e:
            print(f"Error al enviar mensaje: {e}")
            break

def teclaW_detectada(event):
    global stop

    if stop == False:
        stop = True
        print("¡Se ha pulsado STOP!")
        socket_de.send(KO.encode('utf-8'))  # Enviar KO
        print("Enviado: KO")
    
    elif stop == True:
        stop = False
        print("¡Se ha pulsado REANUDAR!")
        hilo_envio = threading.Thread(target=enviarMensajes)
        hilo_envio.start()

def finalTransmision(event):
    global stop
    stop = True
    socket_de.send(FIN.encode('utf-8'))
    print("SE HA AVABADO LA TRANSMISIÓN")
    root.quit()

############## MAIN ################

# Crear la ventana principal
root = tk.Tk()
root.title("Sensor")
root.geometry("100x100")

# Detectar cuando se toque la tecla S
root.bind('<s>', teclaW_detectada)
root.bind('<q>', finalTransmision)

# Conectar al Digital Engine
conectarDigitalEngine()

# Iniciar el hilo para enviar mensajes
hilo_envio = threading.Thread(target=enviarMensajes)
hilo_envio.start()


# Iniciar el bucle principal de la aplicación
root.mainloop()

hilo_envio.join()

