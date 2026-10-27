# EasyCab · Interfaz web

Interfaz de visualización de la simulación EasyCab, incorporada en la parte 2. La portada utiliza Astro y el mapa se actualiza mediante JavaScript consultando la API Flask de la central. React figura entre las dependencias, aunque esta copia no contiene el componente `RealTimeMap` mencionado en la memoria.

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

## Memoria y versión actual

La [memoria de la parte 2 · EasyCab Release](../docs/memorias/Documentaci%C3%B3nParte-2.pdf) describe la interfaz original. Sus capturas están incluidas en el [README principal](../README.md#el-proyecto-en-imágenes) y pueden diferir de la presentación actual.

La portada carga el mapa en un iframe con la ruta `/mapa/`; la memoria utiliza `/mapa.html`. El seguimiento se realiza en ese mapa: las tarjetas de la portada no constituyen un panel independiente de administración. El componente React `RealTimeMap` mencionado en la memoria no está presente en esta copia.
