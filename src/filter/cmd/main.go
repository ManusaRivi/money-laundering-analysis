package main

import (
    "log"
	"os"

    "money-laundering-analysis/src/filter/config"
    "money-laundering-analysis/src/filter/internal"
)

func main() {
    configPath := os.Getenv("CONFIG_FILE")
    cfg, err := config.LoadConfig(configPath)
    if err != nil {
        log.Fatalf("Error al cargar la configuracion: %v", err)
    }

    filterNode, err := internal.NewFilter(*cfg)
    if err != nil {
        log.Fatalf("Error creando el filtro: %v", err)
    }

    filterNode.Run()
}