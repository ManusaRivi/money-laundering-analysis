# money-laundering-analysis

Éste es el repositorio de la solución al trabajo grupal de la materia Sistemas Distribuidos I de la carrera Ingeniería en Informática de la Facultad de Ingeniería, Universidad de Buenos Aires.

## Infraestructura local (Compose generado)

El `docker-compose-dev.yaml` se genera automáticamente a partir de una topología declarativa vía un script en Go, evitando mantener un compose estático.

### Archivos clave

- **`topology.yaml`** — Define la cantidad de clientes (`clients`) y los pipelines con sus workers secuenciales. Cada pipeline es una lista ordenada de workers con nombre y réplicas.
- **`configs/base-compose.yaml.tmpl`** — Template Go (`text/template`) con la infraestructura estática (rabbitmq, gateway, red) y bloques `{{- range .Clients }}` / `{{- range .Workers }}` para inyección dinámica.
- **`scripts/gen_compose.go`** — Generador que parsea `topology.yaml`, aplana los workers con sus réplicas y resuelve el ruteo secuencial (`NEXT_WORKER_PREFIX`/`NEXT_WORKER_AMOUNT` via `i+1`), y ejecuta el template.

### Convenciones de ruteo

Cada worker en la lista recibe automáticamente las variables `NEXT_WORKER_PREFIX` y `NEXT_WORKER_AMOUNT` apuntando al worker siguiente (`i+1`) del mismo pipeline. El último worker de cada pipeline **no** recibe `NEXT_WORKER_*`. Esto permite encadenar workers sin configuración manual.

Variables de entorno inyectadas en cada worker:

| Variable | Descripción |
|---|---|
| `WORKER_PREFIX` | `<pipeline>_<worker_name>` |
| `ID` | Índice de réplica (0..amount-1) |
| `WORKER_AMOUNT` | Total de réplicas del worker |
| `CONFIG_PATH` | `/app/config.yaml` |
| `NEXT_WORKER_PREFIX` | (opcional) Worker siguiente en el pipeline |
| `NEXT_WORKER_AMOUNT` | (opcional) Réplicas del worker siguiente |

### Volúmenes

- Workers: `./configs/<pipeline>/<worker>.yaml:/app/config.yaml`
- Clientes: `./.data:/app/.data` y `./.output/client<ID>:/app/.output`


### Para SNIPER
En `topology.yaml`:
```
env:
  SNIPER: "true"
```
En donde se quiera matar un worker:
```
if os.Getenv("SNIPER") == "true" {
    slog.Warn("[SNIPER] Sleeping to allow sniper to acquire target...")
    time.Sleep(5 * time.Second)
    slog.Info("I survived the Sniper")
}
```