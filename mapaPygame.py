# Levan Kipiani, Aitana Cantó
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

# Tamaño de la tabla de estados
ALTO_TABLA = 150

# Cola para manejar las actualizaciones del mapa
cola_actualizaciones = queue.Queue()

# Inicialización de Pygame
def iniciar_pygame(ancho, alto):
    global ANCHO, ALTO, ANCHO_VENTANA, ALTO_VENTANA, screen, clock, font
    ANCHO, ALTO = ancho, alto

    ANCHO_VENTANA = ANCHO * TAMANO_CELDA
    ALTO_VENTANA = ALTO * TAMANO_CELDA + ALTO_TABLA

    pygame.init()
    screen = pygame.display.set_mode((ANCHO_VENTANA, ALTO_VENTANA))
    pygame.display.set_caption("Mapa de Taxis")
    clock = pygame.time.Clock()  # Controlador de FPS
    font = pygame.font.Font(None, 24)  # Fuente para el texto dentro de las celdas

def dibujar_mapa(mapa_data, estados_taxis, estados_clientes):
    screen.fill(COLOR_FONDO)

    # Dibujar tabla de estados
    dibujar_tabla_estados(estados_taxis, estados_clientes)

    # Dibujar el mapa de celdas
    for y in range(ALTO):
        for x in range(ANCHO):
            try:
                rect = pygame.Rect(x * TAMANO_CELDA, y * TAMANO_CELDA + ALTO_TABLA, TAMANO_CELDA, TAMANO_CELDA)
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
                    
                mapa_data[1][1] = 'X'

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

def dibujar_tabla_estados(estados_taxis, estados_clientes):
    # Títulos de las columnas
    titulo_taxis = font.render("Taxis", True, COLOR_TEXTO)
    screen.blit(titulo_taxis, (10, 10))
    #titulo_clientes = font.render("Clientes", True, COLOR_TEXTO)
    #screen.blit(titulo_clientes, (ANCHO_VENTANA // 2, 10))

    # Dibujar estados de taxis
    y_offset = 40
    botones_taxis = []

    if estados_taxis:
        for estado in estados_taxis:
            texto = f"Id: {estado['id']}, Destino: {estado['destino']}, Estado: {estado['estado']}"
            estado_surface = font.render(texto, True, COLOR_TEXTO)
            screen.blit(estado_surface, (10, y_offset))

            boton_rect = pygame.Rect(300, y_offset - 5, 150, 20)
            pygame.draw.rect(screen, COLOR_AMARILLO, boton_rect)
            boton_texto = font.render("Volver a la base", True, COLOR_TEXTO)
            screen.blit(boton_texto, (boton_rect.x + 5, boton_rect.y))
            botones_taxis.append((boton_rect, estado['id']))

            y_offset += 30

        # Dibujar estados de clientes
        """
        y_offset = 40
        for estado in estados_clientes:
            texto = f"Id: {estado['id']}, Destino: {estado['destino']}, Estado: {estado['estado']}"
            estado_surface = font.render(texto, True, COLOR_TEXTO)
            screen.blit(estado_surface, (ANCHO_VENTANA // 2, y_offset))
            y_offset += 20
        """
def manejar_eventos_botones(evento, botones_taxis):
    """
    if evento.type == pygame.MOUSEBUTTONDOWN:
        mouse_pos = evento.pos
        for boton_rect, taxi_id in botones_taxis:
            if boton_rect.collidepoint(mouse_pos):
                print(f"Taxi {taxi_id} vuelve a la base")  # Acción de "volver a la base"
    """

# Actualizar el mapa: agregar a la cola para ser procesado por el hilo principal
def actualizar_mapa_pygame(mapa_data, estados_taxis, estados_clientes):
    cola_actualizaciones.put((mapa_data, estados_taxis, estados_clientes))

def main_pygame_loop():
    running = True
    while running:
        # Procesar eventos de Pygame
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

        # Ver si hay actualizaciones en la cola
        while not cola_actualizaciones.empty():
            mapa_data, estados_taxis, estados_clientes = cola_actualizaciones.get()
            dibujar_mapa(mapa_data, estados_taxis, estados_clientes)

        # Limitar la tasa de actualización a 30 FPS
        clock.tick(30)

    # Cerrar Pygame y salir del programa
    pygame.quit()
    sys.exit()