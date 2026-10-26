# EasyCab · Interfaz web

Interfaz de visualización de la simulación EasyCab, desarrollada con Astro y React. Consulta el mapa de la central mediante su API Flask.

## Desarrollo

Desde esta carpeta:

```bash
npm ci
npm run dev
```

Abre la dirección indicada por Astro. La central debe estar en ejecución y exponer `http://localhost:5003/mapa`. El navegador consulta la API cada 500 ms; si no está disponible, el mapa muestra un aviso de desconexión.

## Compilación

```bash
npm run build
npm run preview
```

La compilación genera archivos en `dist/`. Publicar esos archivos no pone en marcha la central, Kafka ni los demás procesos Python: el mapa requiere acceso a la API.

## Estructura

- `src/pages/index.astro`: página principal.
- `src/pages/mapa.html`: mapa de la simulación, publicado en `/mapa/`.
- `src/layouts/Layout.astro`: estructura HTML compartida.
- `public/documents/`: guía original de despliegue.

La [guía de ejecución del repositorio](../docs/EJECUCION.md) contiene los comandos actuales de los servicios. El PDF original conserva los comandos de la estructura académica inicial.

## Equipo

Aitana Cantó Rico · Levan Kipiani.
