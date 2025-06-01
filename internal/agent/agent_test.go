package agent

import (
	"os"
	"testing"

	"syncbit/internal/core/types"
)

func TestNewAgent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create temporary config file
	configFile := tmpDir + "/config.yaml"
	configContent := `
debug: true
agent:
  storage:
    base_path: "` + tmpDir + `/data"
    cache:
      ram_limit: 100MB
`
	if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
		t.Fatal(err)
	}

	agent, err := NewAgent(configFile, true)
	if err != nil {
		t.Fatalf("Failed to create agent: %v", err)
	}

	if agent == nil {
		t.Fatal("Agent should not be nil")
	}

	if agent.cfg == nil {
		t.Fatal("Agent config should not be nil")
	}

	if agent.logger == nil {
		t.Fatal("Agent logger should not be nil")
	}

	if agent.cache == nil {
		t.Fatal("Agent cache should not be nil")
	}

	if agent.server == nil {
		t.Fatal("Agent server should not be nil")
	}
}

func TestAgentHelperMethods(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent_helper_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create temporary config file
	configFile := tmpDir + "/config.yaml"
	configContent := `
debug: true
agent:
  storage:
    base_path: "` + tmpDir + `/data"
    cache:
      ram_limit: 100MB
`
	if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
		t.Fatal(err)
	}

	agent, err := NewAgent(configFile, true)
	if err != nil {
		t.Fatalf("Failed to create agent: %v", err)
	}

	t.Run("GetFileKey", func(t *testing.T) {
		key := agent.getFileKey("dataset1", "file1.txt")
		expected := "dataset1:file1.txt"
		if key != expected {
			t.Errorf("Expected %s, got %s", expected, key)
		}
	})

	t.Run("GetCacheStats", func(t *testing.T) {
		stats := agent.getCacheStats()
		if stats.DiskLimit == 0 {
			t.Error("DiskLimit should not be zero")
		}
	})

	t.Run("HasCompleteFile", func(t *testing.T) {
		// Test with non-existent file
		fileKey := agent.getFileKey("test-dataset", "test-file.txt")
		hasFile := agent.hasCompleteFile(fileKey, types.Bytes(1024))
		if hasFile {
			t.Error("Should not have non-existent file")
		}
	})

	t.Run("StoreFileData", func(t *testing.T) {
		testData := []byte("Hello, World!")
		err := agent.storeFileData("test-dataset", "hello.txt", testData)
		if err != nil {
			t.Fatalf("Failed to store file data: %v", err)
		}

		// Verify the file is stored
		fileKey := agent.getFileKey("test-dataset", "hello.txt")
		hasFile := agent.hasCompleteFile(fileKey, types.Bytes(len(testData)))
		if !hasFile {
			t.Error("File should be stored and complete")
		}

		// Verify we can read it back
		data, err := agent.getFileByPath("test-dataset", "hello.txt")
		if err != nil {
			t.Fatalf("Failed to read stored file: %v", err)
		}

		if string(data) != string(testData) {
			t.Errorf("Data mismatch: expected %s, got %s", string(testData), string(data))
		}
	})
}

func TestAgentState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "agent_state_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create temporary config file
	configFile := tmpDir + "/config.yaml"
	configContent := `
debug: true
agent:
  storage:
    base_path: "` + tmpDir + `/data"
    cache:
      ram_limit: 100MB
`
	if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
		t.Fatal(err)
	}

	agent, err := NewAgent(configFile, true)
	if err != nil {
		t.Fatalf("Failed to create agent: %v", err)
	}

	t.Run("BuildAgentState", func(t *testing.T) {
		state := agent.buildAgentState()

		if state.DiskAvailable == 0 {
			t.Error("DiskAvailable should not be zero")
		}

		if state.Assignments == nil {
			t.Error("Assignments should not be nil")
		}

		if state.LocalDatasets == nil {
			t.Error("LocalDatasets should not be nil")
		}

		if state.LastUpdated.IsZero() {
			t.Error("LastUpdated should not be zero")
		}
	})

	t.Run("GetLocalDatasets", func(t *testing.T) {
		datasets := agent.getLocalDatasets()
		if datasets == nil {
			t.Error("Datasets should not be nil")
		}
		// Should be empty initially
		if len(datasets) != 0 {
			t.Errorf("Expected 0 datasets, got %d", len(datasets))
		}
	})
}