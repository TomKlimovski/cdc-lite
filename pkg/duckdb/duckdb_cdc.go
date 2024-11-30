package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBChange represents a single database change from DuckDB
type DuckDBChange struct {
	Timestamp time.Time              `json:"timestamp"`
	Table     string                 `json:"table"`
	Operation string                 `json:"operation"`
	Data      map[string]interface{} `json:"data"`
}

// DuckDBReader handles CDC operations for DuckDB
type DuckDBReader struct {
	sourcePath   string
	metadataDB   *sql.DB
	outputDir    string
	buffer       []*DuckDBChange
	bufferMutex  sync.Mutex
	tables       []string
	pollInterval time.Duration
}

// NewDuckDBReader creates a new DuckDB CDC reader
func NewDuckDBReader(sourcePath, metadataPath, outputDir string) (*DuckDBReader, error) {
	metadataDB, err := sql.Open("duckdb", metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata DuckDB: %v", err)
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	return &DuckDBReader{
		sourcePath:   sourcePath,
		metadataDB:   metadataDB,
		outputDir:    outputDir,
		buffer:       make([]*DuckDBChange, 0),
		pollInterval: 1 * time.Second,
	}, nil
}

func (d *DuckDBReader) initMetadataTables() error {
	_, err := d.metadataDB.Exec(`
		CREATE TABLE IF NOT EXISTS cdc_metadata (
			table_name VARCHAR,
			last_processed_time TIMESTAMP,
			last_file_write_time TIMESTAMP,
			last_file_name VARCHAR
		);

		CREATE TABLE IF NOT EXISTS table_checksums (
			table_name VARCHAR,
			checksum VARCHAR,
			timestamp TIMESTAMP,
			PRIMARY KEY (table_name, timestamp)
		);
	`)
	return err
}

func (d *DuckDBReader) StartMonitoring(ctx context.Context, tables []string) error {
	d.tables = tables

	if err := d.initMetadataTables(); err != nil {
		return fmt.Errorf("failed to initialize metadata tables: %v", err)
	}

	log.Printf("Started monitoring tables: %v", tables)
	// Start monitoring goroutine
	go d.monitorTables(ctx)

	return nil
}

func (d *DuckDBReader) monitorTables(ctx context.Context) {
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Create a temporary copy of the database for reading
			tmpDB := fmt.Sprintf("%s.tmp.%d", d.sourcePath, time.Now().UnixNano())
			if err := d.copyFile(d.sourcePath, tmpDB); err != nil {
				log.Printf("Error copying database: %v", err)
				continue
			}

			// Open the temporary database
			sourceDB, err := sql.Open("duckdb", tmpDB)
			if err != nil {
				log.Printf("Error opening temporary database: %v", err)
				os.Remove(tmpDB)
				continue
			}

			// Check each table for changes
			for _, table := range d.tables {
				if err := d.checkTableChanges(sourceDB, table); err != nil {
					log.Printf("Error checking changes for table %s: %v\n", table, err)
				}
			}

			// Clean up
			sourceDB.Close()
			os.Remove(tmpDB)
		}
	}
}

func (d *DuckDBReader) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func (d *DuckDBReader) getTableColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	return columns, nil
}

func (d *DuckDBReader) checkTableChanges(db *sql.DB, table string) error {
	// Get table columns
	columns, err := d.getTableColumns(db, table)
	if err != nil {
		return fmt.Errorf("failed to get table columns: %v", err)
	}

	// Build checksum query using all columns
	hashParts := make([]string, len(columns))
	for i, col := range columns {
		hashParts[i] = fmt.Sprintf("COALESCE(CAST(%s AS VARCHAR), '')", col)
	}
	hashExpr := strings.Join(hashParts, " || ',' || ")
	
	query := fmt.Sprintf(`
		SELECT md5(
			CAST(COUNT(*) AS VARCHAR) || ',' ||
			CAST(SUM(hash(%s)) AS VARCHAR)
		) FROM %s`, hashExpr, table)

	// Calculate current table checksum
	var currentChecksum string
	err = db.QueryRow(query).Scan(&currentChecksum)
	if err != nil {
		return fmt.Errorf("failed to calculate table checksum: %v", err)
	}

	// Compare with last known checksum
	var lastChecksum string
	err = d.metadataDB.QueryRow(`
		SELECT checksum 
		FROM table_checksums 
		WHERE table_name = ? 
		ORDER BY timestamp DESC 
		LIMIT 1`, table).Scan(&lastChecksum)

	if err == sql.ErrNoRows || lastChecksum != currentChecksum {
		log.Printf("Changes detected in table %s", table)
		// Changes detected, capture current state
		if err := d.captureTableState(db, table); err != nil {
			return fmt.Errorf("failed to capture table state: %v", err)
		}

		// Update checksum
		_, err = d.metadataDB.Exec(`
			INSERT INTO table_checksums (table_name, checksum, timestamp)
			VALUES (?, ?, ?)`,
			table, currentChecksum, time.Now())
		if err != nil {
			return fmt.Errorf("failed to update checksum: %v", err)
		}
	}

	return nil
}

func (d *DuckDBReader) captureTableState(db *sql.DB, table string) error {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return fmt.Errorf("failed to query table: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		data := make(map[string]interface{})
		for i, col := range columns {
			data[col] = values[i]
		}

		change := &DuckDBChange{
			Timestamp: time.Now(),
			Table:     table,
			Operation: "SNAPSHOT", // Since we're capturing current state
			Data:      data,
		}

		d.bufferMutex.Lock()
		d.buffer = append(d.buffer, change)
		d.bufferMutex.Unlock()
	}

	return d.writeBufferToFile()
}

func (d *DuckDBReader) writeBufferToFile() error {
	d.bufferMutex.Lock()
	defer d.bufferMutex.Unlock()

	if len(d.buffer) == 0 {
		return nil
	}

	timestamp := time.Now().UTC()
	fileName := fmt.Sprintf("changes_%s.jsonl", timestamp.Format("20060102_150405"))
	filePath := filepath.Join(d.outputDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	for _, change := range d.buffer {
		data, err := json.Marshal(change)
		if err != nil {
			continue
		}

		if _, err := file.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("failed to write to file: %v", err)
		}
	}

	log.Printf("Written changes to file: %s", filePath)
	// Clear buffer after successful write
	d.buffer = make([]*DuckDBChange, 0)

	return nil
}

func (d *DuckDBReader) Close() error {
	if err := d.metadataDB.Close(); err != nil {
		return fmt.Errorf("failed to close metadata DB: %v", err)
	}
	return nil
}
