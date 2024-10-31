import pygame
import sys
import queue

# Definir constantes y colores
COLOR_FONDO = (211, 211, 211)  # Gris claro
COLOR_CLIENTE = (0, 0, 255)     # Azul
COLOR_DESTINO = (255, 255, 0)  
COLOR_BORDE = (0, 0, 0)         # Negro
COLOR_TEXTO = (0, 0, 0)         # Negro (para el texto dentro de las casillas)
COLOR_VERDE = (0, 255, 0)
COLOR_ROJO = (255, 0, 0)
COLOR_AMARILLO = (255, 255, 0)

# Tamaño de las celdas
TAMANO_CELDA = 30
ANCHO_BORDE = 1

# Cola para manejar las actualizaciones del mapa
cola_actualizaciones = queue.Queue()

# Inicialización de Pygame
def iniciar_pygame(ancho, alto):
    global ANCHO, ALTO, ANCHO_VENTANA, ALTO_VENTANA, screen, clock, font
    ANCHO, ALTO = ancho, alto

    ANCHO_VENTANA = ANCHO * TAMANO_CELDA
    ALTO_VENTANA = ALTO * TAMANO_CELDA

    pygame.init()
    screen = pygame.display.set_mode((ANCHO_VENTANA, ALTO_VENTANA))
    pygame.display.set_caption("Mapa de Taxis")
    clock = pygame.time.Clock()  # Controlador de FPS
    font = pygame.font.Font(None, 24)  # Fuente para el texto dentro de las celdas

def dibujar_mapa(mapa_data):
    screen.fill(COLOR_FONDO)

    for y in range(ALTO):
        for x in range(ANCHO):
            try:
                rect = pygame.Rect(x * TAMANO_CELDA, y * TAMANO_CELDA, TAMANO_CELDA, TAMANO_CELDA)
                if mapa_data[x][y] == ' ':
                    color = COLOR_FONDO
                    texto = ''
                elif mapa_data[x][y][0].isdigit():  # Taxi
                    if mapa_data[x][y][1] == 'r':
                        color = COLOR_ROJO
                    elif mapa_data[x][y][1] == 'a':
                        color = COLOR_AMARILLO
                    else:
                        color = COLOR_VERDE
                    texto = mapa_data[x][y][0]
                elif mapa_data[x][y].isalpha() and mapa_data[x][y].isupper():  # Destino
                    color = COLOR_DESTINO
                    texto = mapa_data[x][y]
                elif mapa_data[x][y].isalpha() and mapa_data[x][y].islower():  # Cliente
                    color = COLOR_CLIENTE
                    texto = mapa_data[x][y]
                else:
                    color = COLOR_FONDO
                    texto = ''

                # Dibujar la celda con el color correspondiente
                pygame.draw.rect(screen, color, rect)
                pygame.draw.rect(screen, COLOR_BORDE, rect, ANCHO_BORDE)

                # Dibujar el texto en la celda si no está vacío
                if texto:
                    text_surface = font.render(texto, True, COLOR_TEXTO)
                    text_rect = text_surface.get_rect(center=rect.center)
                    screen.blit(text_surface, text_rect)
            except Exception as e:
                print(f"Error representar el mapa --> {mapa_data[x][y]}")

    pygame.display.flip()

# Actualizar el mapa: agregar a la cola para ser procesado por el hilo principal
def actualizar_mapa_pygame(mapa_data):
    cola_actualizaciones.put(mapa_data)

def main_pygame_loop():
    running = True
    while running:
        # Procesar eventos de Pygame
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

        # Ver si hay actualizaciones en la cola
        while not cola_actualizaciones.empty():
            mapa_data = cola_actualizaciones.get()
            dibujar_mapa(mapa_data)

        # Limitar la tasa de actualización a 30 FPS
        clock.tick(30)

    # Cerrar Pygame y salir del programa
    pygame.quit()
    sys.exit()
