# Consumo eléctrico

Monitor web local del consumo eléctrico con precios PVPC (REE/ESIOS).

Lee tres medidores cada 5 segundos y guarda el histórico de cada uno en SQLite:

| Zona | Equipo | Conexión |
|---|---|---|
| principal | Shelly Pro EM-50 | HTTP RPC, 192.168.4.175 |
| acuario | Shelly PM Mini Gen3 | HTTP RPC, 192.168.4.203 |
| schneider | Schneider PowerLogic PM710 | Modbus TCP vía USR-W610, 192.168.100.206:502 |

## Uso

En Windows, ejecuta `INSTALAR_Y_ARRANCAR.bat` y abre http://127.0.0.1:5000.

- `/` monitor en tiempo real por zona, con estimación de factura.
- `/historico` consulta del histórico por rango de fechas, agrupado por hora, día o mes, con CSV.
- `/ajustes` tarifa y token de ESIOS.

Las instrucciones completas están en [LEEME.txt](LEEME.txt).

## Datos locales

Las bases de datos (`shelly.db`, `acuario.db`, `schneider.db`) y `config.json` no se suben al repositorio.
Al actualizar, copia esos archivos de la instalación anterior.

## Ramas

- `main`: versión estable, la que se instala.
- `desarrollo`: cambios en curso; se integran en `main` cuando están probados.
