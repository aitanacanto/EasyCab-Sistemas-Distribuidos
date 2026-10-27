# Configuración local

Copia `apikey.example.txt` como `apikey.txt` y sustituye su contenido por tu clave de OpenWeather, en una sola línea.

Los archivos de credenciales de esta carpeta se excluyen de nuevas incorporaciones a Git. Las claves de sesión, los tokens y el estado de la simulación se guardan en `runtime/`.

Usa una clave propia y vigente. Una credencial que haya aparecido en un repositorio debe revocarse en su proveedor y sustituirse; moverla a esta carpeta o ignorarla no la invalida ni elimina sus versiones del historial. No reutilices claves privadas expuestas.

Los módulos actuales no configuran TLS y no requieren incluir certificados ni claves privadas PEM en el repositorio. Si añades TLS en otra versión, genera credenciales nuevas y mantenlas fuera de Git.
