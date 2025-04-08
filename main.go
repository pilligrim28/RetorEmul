package main

import (
	"log"
	"retroemul/config"
    "retroemul/internal/models"
	
)

func main() {
	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	repeater := NewRepeater(config)
	defer repeater.Stop()

	if err := repeater.Start(); err != nil {
		log.Fatalf("Failed to start repeater: %v", err)
	}

	log.Println("Repeater started successfully")
	select {} // Infinite wait
}

