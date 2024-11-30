package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Configuration
	sourcePath := "./source.duckdb"
	metadataPath := "./metadata.duckdb"
	outputDir := "./cdc_output"

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize DuckDB reader
	reader, err := NewDuckDBReader(sourcePath, metadataPath, outputDir)
	if err != nil {
		log.Fatalf("Failed to create DuckDB reader: %v", err)
	}
	defer reader.Close()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Start monitoring tables
	tables := []string{"your_table_name"} // Replace with actual table names
	if err := reader.StartMonitoring(ctx, tables); err != nil {
		log.Fatalf("Failed to start monitoring: %v", err)
	}

	// Wait for context cancellation
	<-ctx.Done()
	log.Println("CDC monitoring stopped")
}