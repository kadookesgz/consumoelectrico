# Consumo eléctrico

Monitor web local del consumo eléctrico con precios PVPC (REE/ESIOS).

Lee tres medidores cada 5 segundos y guarda el histórico de cada uno en SQLite:

| Zona | Equipo | Conexión |
|---|---|---|
| principal | Shelly Pro EM-50 | HTTP RPC, 192.168.4.175 |
| acuario | Shelly PM Mini Gen3 | HTTP RPC, 192.168.4.203 |
| schneider | Schneider PowerLogic PM710 | Modbus TCP vía USR-W610, 192.168.100.206:502 |

Los equipos se configuran en Ajustes → Equipos (se guardan en `config.json`); la tabla muestra los valores por defecto.

## Uso

En Windows, ejecuta `INSTALAR_Y_ARRANCAR.bat` y abre http://127.0.0.1:5000.
Para que arranque solo al encender el ordenador, ejecuta `INICIO_AUTOMATICO_ACTIVAR.bat`.

- `/` monitor en tiempo real por zona, con avisos y estimación de factura.
- `/historico` consulta del histórico por rango de fechas, agrupado por hora, día o mes, con comparación con el periodo anterior, reparto entre zonas y CSV.
- `/ajustes` tarifa, equipos, avisos (Telegram), conservación del histórico y contraseña.

## Estructura

- `shelly_pvpc_web.py`: aplicación Flask (lectura de medidores, precios PVPC, avisos y web).
- `templates/`: páginas HTML.
- `static/`: estilos y Chart.js 4.4.1 (licencia MIT), para que las gráficas funcionen sin Internet.

Cada zona guarda en su base de datos las lecturas de 5 s (`measurements`) y un resumen por horas (`hourly`) que usan el monitor y el histórico. Los precios PVPC y los avisos se guardan en `shelly.db`.

Las instrucciones completas están en [LEEME.txt](LEEME.txt).

## Datos locales

Las bases de datos (`shelly.db`, `acuario.db`, `schneider.db`), `config.json` (tokens y contraseña) y `app.log` no se suben al repositorio.
Al actualizar, copia esos archivos de la instalación anterior.

## Ramas

- `main`: versión estable, la que se instala.
- `desarrollo`: cambios en curso; se integran en `main` cuando están probados.
