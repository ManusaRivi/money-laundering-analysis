# src/cmd/node/Dockerfile

# Etapa 1: Build
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Descargar dependencias aprovechando la caché de Docker
COPY go.mod go.sum ./
RUN go mod download

# Copiar todo el código fuente
COPY . .

# Compilar el orquestador (Fat Binary)
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/node_bin ./src/cmd/node/main.go

# Etapa 2: Imagen final
FROM alpine:latest

WORKDIR /app

# Copiar el binario desde la etapa anterior
COPY --from=builder /app/node_bin .

# El entrypoint ejecuta el binario. 
# La configuración se inyecta vía volumen y variable de entorno.
ENTRYPOINT ["/app/node_bin"]