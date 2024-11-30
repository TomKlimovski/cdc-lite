package duckdb

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var testLock sync.Mutex

func setupTestEnvironment(t *testing.T) (string, string, string) {
	testLock.Lock() // Ensure only one test runs at a time

	// Create temporary directories for test
	tmpDir, err := os.MkdirTemp("", "duckdb-cdc-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	sourcePath := filepath.Join(tmpDir, "source.duckdb")
	metadataPath := filepath.Join(tmpDir, "metadata.duckdb")
	outputDir := filepath.Join(tmpDir, "output")

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// Create source database with test data
	db, err := sql.Open("duckdb", sourcePath)
	if err != nil {
		t.Fatalf("Failed to create source database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			email VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		INSERT INTO users (id, name, email) VALUES 
			(1, 'John Doe', 'john@example.com'),
			(2, 'Jane Smith', 'jane@example.com');
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	return sourcePath, metadataPath, outputDir
}

func cleanupTestEnvironment(t *testing.T, paths ...string) {
	defer testLock.Unlock() // Release the lock when cleanup is done
	for _, path := range paths {
		if err := os.RemoveAll(filepath.Dir(path)); err != nil {
			t.Errorf("Failed to cleanup test environment: %v", err)
		}
	}
}

func TestNewDuckDBReader(t *testing.T) {
	sourcePath, metadataPath, outputDir := setupTestEnvironment(t)
	defer cleanupTestEnvironment(t, sourcePath)

	reader, err := NewDuckDBReader(sourcePath, metadataPath, outputDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDBReader: %v", err)
	}
	defer reader.Close()

	if reader.sourcePath != sourcePath {
		t.Errorf("Expected sourcePath %s, got %s", sourcePath, reader.sourcePath)
	}

	if reader.outputDir != outputDir {
		t.Errorf("Expected outputDir %s, got %s", outputDir, reader.outputDir)
	}

	// Check if output directory was created
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		t.Errorf("Output directory was not created")
	}
}

func TestInitMetadataTables(t *testing.T) {
	sourcePath, metadataPath, outputDir := setupTestEnvironment(t)
	defer cleanupTestEnvironment(t, sourcePath)

	reader, err := NewDuckDBReader(sourcePath, metadataPath, outputDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDBReader: %v", err)
	}
	defer reader.Close()

	if err := reader.initMetadataTables(); err != nil {
		t.Fatalf("Failed to initialize metadata tables: %v", err)
	}

	// Verify tables were created
	var tableCount int
	err = reader.metadataDB.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='table' 
		AND (name='cdc_metadata' OR name='table_checksums')
	`).Scan(&tableCount)

	if err != nil {
		t.Fatalf("Failed to query metadata tables: %v", err)
	}

	if tableCount != 2 {
		t.Errorf("Expected 2 metadata tables, got %d", tableCount)
	}
}

func TestChangeDetection(t *testing.T) {
	sourcePath, metadataPath, outputDir := setupTestEnvironment(t)
	defer cleanupTestEnvironment(t, sourcePath)

	reader, err := NewDuckDBReader(sourcePath, metadataPath, outputDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDBReader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start monitoring
	err = reader.StartMonitoring(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to start monitoring: %v", err)
	}

	// Make a change to the source database
	sourceDB, err := sql.Open("duckdb", sourcePath)
	if err != nil {
		t.Fatalf("Failed to open source database: %v", err)
	}

	_, err = sourceDB.Exec(`
		INSERT INTO users (id, name, email) 
		VALUES (3, 'Bob Wilson', 'bob@example.com')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}
	sourceDB.Close()

	// Wait for change detection
	time.Sleep(2 * time.Second)

	// Check if changes were detected and written
	files, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("Failed to read output directory: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("No change files were created")
	}

	// Read the latest change file
	latestFile := files[len(files)-1]
	file, err := os.Open(filepath.Join(outputDir, latestFile.Name()))
	if err != nil {
		t.Fatalf("Failed to open change file: %v", err)
	}
	defer file.Close()

	// Read the file line by line
	scanner := bufio.NewScanner(file)
	changeFound := false
	for scanner.Scan() {
		var change DuckDBChange
		if err := json.Unmarshal(scanner.Bytes(), &change); err != nil {
			t.Fatalf("Failed to parse change line: %v", err)
		}

		if change.Table == "users" {
			changeFound = true
			// Verify some basic properties of the change
			if change.Operation != "SNAPSHOT" {
				t.Errorf("Expected operation 'SNAPSHOT', got %s", change.Operation)
			}
			if change.Data == nil {
				t.Error("Change data is nil")
			}
		}
	}

	if !changeFound {
		t.Error("No change record found for users table")
	}

	// Wait for goroutines to finish
	cancel()
	time.Sleep(100 * time.Millisecond)
	reader.Close()
}

func TestFileWriting(t *testing.T) {
	sourcePath, metadataPath, outputDir := setupTestEnvironment(t)
	defer cleanupTestEnvironment(t, sourcePath)

	reader, err := NewDuckDBReader(sourcePath, metadataPath, outputDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDBReader: %v", err)
	}
	defer reader.Close()

	// Create a test change
	change := &DuckDBChange{
		Timestamp: time.Now(),
		Table:     "users",
		Operation: "SNAPSHOT",
		Data: map[string]interface{}{
			"id":    1,
			"name":  "Test User",
			"email": "test@example.com",
		},
	}

	reader.buffer = append(reader.buffer, change)

	// Write buffer to file
	if err := reader.writeBufferToFile(); err != nil {
		t.Fatalf("Failed to write buffer to file: %v", err)
	}

	// Check if file was created
	files, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("Failed to read output directory: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("Expected 1 output file, got %d", len(files))
	}

	// Read and verify file content
	file, err := os.Open(filepath.Join(outputDir, files[0].Name()))
	if err != nil {
		t.Fatalf("Failed to open output file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		t.Fatal("Failed to read line from output file")
	}

	var writtenChange DuckDBChange
	if err := json.Unmarshal(scanner.Bytes(), &writtenChange); err != nil {
		t.Fatalf("Failed to parse output file: %v", err)
	}

	if writtenChange.Table != change.Table {
		t.Errorf("Expected table %s, got %s", change.Table, writtenChange.Table)
	}

	if writtenChange.Operation != change.Operation {
		t.Errorf("Expected operation %s, got %s", change.Operation, writtenChange.Operation)
	}
}

func TestCopyFile(t *testing.T) {
	sourcePath, metadataPath, outputDir := setupTestEnvironment(t)
	defer cleanupTestEnvironment(t, sourcePath)

	reader, err := NewDuckDBReader(sourcePath, metadataPath, outputDir)
	if err != nil {
		t.Fatalf("Failed to create DuckDBReader: %v", err)
	}
	defer reader.Close()

	// Create a temporary file for testing
	tmpFile := filepath.Join(filepath.Dir(sourcePath), "test.tmp")
	err = reader.copyFile(sourcePath, tmpFile)
	if err != nil {
		t.Fatalf("Failed to copy file: %v", err)
	}
	defer os.Remove(tmpFile)

	// Verify the copy exists
	if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
		t.Error("Copied file does not exist")
	}

	// Verify the content
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		t.Fatalf("Failed to stat source file: %v", err)
	}

	copyInfo, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Failed to stat copy file: %v", err)
	}

	if sourceInfo.Size() != copyInfo.Size() {
		t.Errorf("Copy size %d does not match source size %d", copyInfo.Size(), sourceInfo.Size())
	}
}
