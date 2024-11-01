package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	_ "github.com/marcboeker/go-duckdb"
)

// Change represents a single database change
type Change struct {
	LSN       uint64                  `json:"lsn"`
	Timestamp time.Time              `json:"timestamp"`
	Table     string                 `json:"table"`
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
}

// FileSummary represents statistics for a single output file
type FileSummary struct {
	FileName        string            `json:"file_name"`
	TimeStart       time.Time         `json:"time_start"`
	TimeEnd         time.Time         `json:"time_end"`
	ChangeCount     int               `json:"change_count"`
	TableStats      map[string]int    `json:"table_stats"`
	OperationStats  map[string]int    `json:"operation_stats"`
	StartLSN        string            `json:"start_lsn"`
	EndLSN          string            `json:"end_lsn"`
	SizeBytes       int64             `json:"size_bytes"`
}

// MetadataSummary represents the overall CDC process metadata
type MetadataSummary struct {
	LastUpdate      time.Time              `json:"last_update"`
	TotalFiles      int                    `json:"total_files"`
	TotalChanges    int                    `json:"total_changes"`
	RecentFiles     []FileSummary          `json:"recent_files"`
	GlobalStats     map[string]interface{} `json:"global_stats"`
}

type WALReader struct {
	conn           *pgx.Conn
	slotName       string
	publication    string
	metadataDB     *sql.DB
	outputDir      string
	buffer         []*Change
	bufferMutex    sync.Mutex
	currentSummary *FileSummary
	metadata       *MetadataSummary
	summaryMutex   sync.Mutex
}

func NewWALReader(pgConnStr, duckdbPath, outputDir string) (*WALReader, error) {
	conn, err := pgx.Connect(context.Background(), pgConnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %v", err)
	}

	metadataDB, err := sql.Open("duckdb", duckdbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %v", err)
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	reader := &WALReader{
		conn:        conn,
		slotName:    "cdc_slot",
		publication: "cdc_pub",
		metadataDB:  metadataDB,
		outputDir:   outputDir,
		buffer:      make([]*Change, 0),
		metadata:    &MetadataSummary{
			RecentFiles: make([]FileSummary, 0),
			GlobalStats: make(map[string]interface{}),
		},
	}

	if err := reader.initMetadataTables(); err != nil {
		return nil, err
	}

	return reader, nil
}

func (w *WALReader) initMetadataTables() error {
	_, err := w.metadataDB.Exec(`
		CREATE TABLE IF NOT EXISTS cdc_metadata (
			slot_name VARCHAR,
			last_lsn VARCHAR,
			last_processed_time TIMESTAMP,
			last_file_write_time TIMESTAMP,
			last_file_name VARCHAR
		);

		CREATE TABLE IF NOT EXISTS change_history (
			lsn VARCHAR,
			table_name VARCHAR,
			operation VARCHAR,
			timestamp TIMESTAMP,
			data JSON,
			file_name VARCHAR,
			PRIMARY KEY (lsn, table_name)
		);
	`)
	return err
}

func (w *WALReader) StartReplication(ctx context.Context, tables []string) error {
	// Create publication if not exists
	_, err := w.conn.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s",
		w.publication,
		tables[0], // For example purposes, using first table
	))
	if err != nil {
		log.Printf("Publication might already exist: %v", err)
	}

	// Create replication slot
	_, err = w.conn.Exec(ctx, fmt.Sprintf(
		"SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
		w.slotName,
	))
	if err != nil {
		log.Printf("Replication slot might already exist: %v", err)
	}

	// Start file writer goroutine
	go w.startFileWriter(ctx)

	return w.startStreaming(ctx)
}

func (w *WALReader) startFileWriter(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.writeBufferToFile(); err != nil {
				log.Printf("Error writing to file: %v\n", err)
			}
		}
	}
}

func (w *WALReader) startStreaming(ctx context.Context) error {
	sysident, err := pglogrepl.IdentifySystem(ctx, w.conn)
	if err != nil {
		return fmt.Errorf("identify system: %v", err)
	}

	err = pglogrepl.StartReplication(ctx, w.conn, w.slotName, sysident.XLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", w.publication),
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %v", err)
	}

	return w.processWALMessages(ctx)
}

func (w *WALReader) processWALMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := w.conn.ReceiveMessage(ctx)
			if err != nil {
				return fmt.Errorf("receive message: %v", err)
			}

			switch msg := msg.(type) {
			case *pgx.ReplicationMessage:
				change := &Change{
					LSN:       uint64(msg.WalMessage.WalStart),
					Timestamp: time.Now().UTC(),
					// You'll need to implement proper WAL message decoding here
					// This is a simplified version
					Table:     "example_table",
					Operation: "INSERT",
					Data:      make(map[string]interface{}),
				}

				w.bufferMutex.Lock()
				w.buffer = append(w.buffer, change)
				w.bufferMutex.Unlock()
			}
		}
	}
}

func (w *WALReader) writeBufferToFile() error {
	w.bufferMutex.Lock()
	w.summaryMutex.Lock()
	defer w.summaryMutex.Unlock()

	if len(w.buffer) == 0 {
		w.bufferMutex.Unlock()
		return nil
	}

	timestamp := time.Now().UTC()
	fileName := fmt.Sprintf("changes_%s.jsonl", timestamp.Format("20060102_150405"))
	filePath := filepath.Join(w.outputDir, fileName)

	summary := &FileSummary{
		FileName:       fileName,
		TimeStart:      timestamp,
		TableStats:     make(map[string]int),
		OperationStats: make(map[string]int),
		StartLSN:      fmt.Sprintf("%X", w.buffer[0].LSN),
	}

	file, err := os.Create(filePath)
	if err != nil {
		w.bufferMutex.Unlock()
		return fmt.Errorf("failed to create file: %v", err)
	}

	for _, change := range w.buffer {
		data, err := json.Marshal(change)
		if err != nil {
			continue
		}

		if _, err := file.Write(append(data, '\n')); err != nil {
			w.bufferMutex.Unlock()
			file.Close()
			return fmt.Errorf("failed to write to file: %v", err)
		}

		summary.ChangeCount++
		summary.TableStats[change.Table]++
		summary.OperationStats[change.Operation]++
	}

	summary.TimeEnd = time.Now().UTC()
	summary.EndLSN = fmt.Sprintf("%X", w.buffer[len(w.buffer)-1].LSN)

	fileInfo, err := file.Stat()
	if err == nil {
		summary.SizeBytes = fileInfo.Size()
	}

	file.Close()

	w.buffer = make([]*Change, 0)
	w.bufferMutex.Unlock()

	return w.updateMetadataSummary(summary)
}

func (w *WALReader) updateMetadataSummary(newSummary *FileSummary) error {
	w.metadata.LastUpdate = time.Now().UTC()
	w.metadata.TotalFiles++
	w.metadata.TotalChanges += newSummary.ChangeCount

	w.metadata.RecentFiles = append(w.metadata.RecentFiles, *newSummary)
	if len(w.metadata.RecentFiles) > 10 {
		w.metadata.RecentFiles = w.metadata.RecentFiles[1:]
	}

	w.metadata.GlobalStats = w.calculateGlobalStats()

	metadataPath := filepath.Join(w.outputDir, "cdc_metadata_summary.json")
	tmpPath := metadataPath + ".tmp"

	metadataFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %v", err)
	}

	encoder := json.NewEncoder(metadataFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(w.metadata); err != nil {
		metadataFile.Close()
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	metadataFile.Close()

	if err := os.Rename(tmpPath, metadataPath); err != nil {
		return fmt.Errorf("failed to update metadata file: %v", err)
	}

	return nil
}

func (w *WALReader) calculateGlobalStats() map[string]interface{} {
	stats := make(map[string]interface{})

	if len(w.metadata.RecentFiles) >= 2 {
		first := w.metadata.RecentFiles[0]
		last := w.metadata.RecentFiles[len(w.metadata.RecentFiles)-1]
		duration := last.TimeEnd.Sub(first.TimeStart).Minutes()
		if duration > 0 {
			stats["changes_per_minute"] = float64(last.ChangeCount) / duration
		}
	}

	tableStats := make(map[string]int)
	opStats := make(map[string]int)

	for _, summary := range w.metadata.RecentFiles {
		for table, count := range summary.TableStats {
			tableStats[table] += count
		}
		for op, count := range summary.OperationStats {
			opStats[op] += count
		}
	}

	stats["table_totals"] = tableStats
	stats["operation_totals"] = opStats

	var totalSize int64
	for _, summary := range w.metadata.RecentFiles {
		totalSize += summary.SizeBytes
	}
	stats["total_size_bytes"] = totalSize

	return stats
}

func main() {
	// Configuration
	pgConnStr := "postgres://user:pass@localhost:5432/db?replication=database"
	duckdbPath := "./cdc_metadata.duckdb"
	outputDir := "./cdc_output"

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize WAL reader
	reader, err := NewWALReader(pgConnStr, duckdbPath, outputDir)
	if err != nil {
		log.Fatalf("Failed to create WAL reader: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Start replication
	tables := []string{"your_table_name"}
	if err := reader.StartReplication(ctx, tables); err != nil {
		log.Fatalf("Failed to start replication: %v", err)
	}
}
