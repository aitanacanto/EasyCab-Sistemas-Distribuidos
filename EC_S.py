import socket
import sys
import tkinter as tk
import threading
import time

# Entradas: IP y Puerto del DigitalEngine
if len(sys.argv) != 3:
    print("ERROR: Número de argumentos erróneos")    
    print("Formato correcto -> python3 EC_S.py <IP DigitalEngine> <Puerto DigitalEngine>")
    exit()

argumentos = sys.argv

FIN = "<FIN>"
OK = "OK"
KO = "KO"

IP_DE = argumentos[1]
PUERTO_DE = int(argumentos[2])

############ FUNCIONES #############

# Evento para sincronización de hilos
stop_event = threading.Event()
mensaje_evento = threading.Event()
mensaje_actual = OK

def conectarDigitalEngine():
    global socket_de
    try:
        socket_de = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_de.connect((IP_DE, PUERTO_DE))
        print(f"CONECTADO AL SERVIDOR CENTRAL EN [{IP_DE} : {PUERTO_DE}]")
    except Exception as e:
        print(f"Error al conectar con Digital Engine: {e}")
        sys.exit(1)

def enviarMensajes():
    global mensaje_actual
    while not stop_event.is_set():
        try:
            mensaje_evento.wait()  # Espera a que haya un mensaje listo para enviar
            socket_de.send(mensaje_actual.encode('utf-8'))
            print("Enviado: " + mensaje_actual)
            time.sleep(1)
        except Exception as e:
            print(f"Error al enviar mensaje: {e}")
            break

def teclaS_detectada(event):
    global mensaje_actual

    if mensaje_actual == OK:
        mensaje_actual = KO
        print("¡Se ha pulsado STOP!")
    else:
        mensaje_actual = OK
        print("¡Se ha pulsado REANUDAR!")
    
    mensaje_evento.set()  # Activa el evento para que el hilo de envío envíe el nuevo mensaje

def finalTransmision(event):
    try:
        socket_de.send(FIN.encode('utf-8'))
        print("SE HA ACABADO LA TRANSMISIÓN")
        stop_event.set()  # Indica al hilo de envío que debe finalizar
    except Exception as e:
        print(f"Error al enviar FIN: {e}")
    finally:
        socket_de.close()  # Cierra el socket
        root.quit()

############## MAIN ################

# Crear la ventana principal
root = tk.Tk()
root.title("Sensor")
root.geometry("100x100")

# Detectar cuando se toque la tecla S
root.bind('<s>', teclaS_detectada)
root.bind('<q>', finalTransmision)

# Conectar al Digital Engine
conectarDigitalEngine()

# Iniciar el hilo para enviar mensajes
stop_event.clear()
mensaje_evento.set()  # Iniciar con el mensaje OK
hilo_envio = threading.Thread(target=enviarMensajes)
hilo_envio.start()

# Iniciar el bucle principal de la aplicación
root.mainloop()

hilo_envio.join()