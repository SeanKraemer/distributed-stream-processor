package storage

import (
	"os"
	"path/filepath"
	"sync"
)

const StorageDir = "./hydfs_storage"

type Manager struct {
	mu sync.RWMutex
}

func NewManager() *Manager {
	// Create storage directory if it doesn't exist
	os.MkdirAll(StorageDir, 0755)
	return &Manager{}
}

// WriteFile writes file content to disk
func (m *Manager) WriteFile(filename string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := filepath.Join(StorageDir, filename)
	return os.WriteFile(path, data, 0644)
}

// ReadFile reads file content from disk
func (m *Manager) ReadFile(filename string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	path := filepath.Join(StorageDir, filename)
	return os.ReadFile(path)
}

// FileExists checks if file exists
func (m *Manager) FileExists(filename string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	path := filepath.Join(StorageDir, filename)
	_, err := os.Stat(path)
	return err == nil
}

// DeleteFile removes a file from disk
func (m *Manager) DeleteFile(filename string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := filepath.Join(StorageDir, filename)
	return os.Remove(path)
}

// ListFiles returns all stored filenames
func (m *Manager) ListFiles() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	files, err := os.ReadDir(StorageDir)
	if err != nil {
		return nil, err
	}

	var filenames []string
	for _, f := range files {
		if !f.IsDir() {
			filenames = append(filenames, f.Name())
		}
	}
	return filenames, nil
}
