package main

import (
	"encoding/json"
	"io/ioutil"
)

// Test configuration structures (shared across all tests)
type Configuration struct {
	Debug  bool      `json:"Debug"`
	Notify []Service `json:"Notify"`
}

type Service struct {
	Service            string   `json:"Service"`
	Hosts              []string `json:"Hosts"`
	Context            string   `json:"Context"`
	Username           string   `json:"Username"`
	Password           string   `json:"Password"`
	Critical           bool     `json:"Critical"`
	Skip               bool     `json:"Skip"`
	Timeout            int      `json:"Timeout"`
	Retries            int      `json:"Retries"`
	Secure             bool     `json:"Secure"`
	Connections        int      `json:"Connections"`
	BatchingEnabled    bool     `json:"BatchingEnabled"`
	MaxBatchSize       int      `json:"MaxBatchSize"`
	BatchFlushInterval int      `json:"BatchFlushInterval"`
	EnableCompression  bool     `json:"EnableCompression"`
}

// Helper functions (shared across all tests)
func loadConfiguration() (*Configuration, error) {
	data, err := ioutil.ReadFile("config.json")
	if err != nil {
		return nil, err
	}

	var config Configuration
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func getStringValue(values map[string]interface{}, key, defaultValue string) string {
	if val, ok := values[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return defaultValue
}

func getFloatValue(values map[string]interface{}, key string, defaultValue float64) float64 {
	if val, ok := values[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return defaultValue
}

func getIntValue(values map[string]interface{}, key string, defaultValue int) int {
	if val, ok := values[key]; ok {
		switch v := val.(type) {
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
	}
	return defaultValue
}

func formatParamsAsJSON(values map[string]interface{}) string {
	// Create a simplified params object for storage
	params := make(map[string]interface{})
	for key, value := range values {
		// Store non-core fields as params
		if key != "vid" && key != "sid" && key != "uid" && key != "url" &&
			key != "utm_source" && key != "utm_medium" && key != "utm_campaign" &&
			key != "country" && key != "region" && key != "city" && key != "lat" && key != "lon" {
			params[key] = value
		}
	}

	if len(params) == 0 {
		return "{}"
	}

	jsonBytes, err := json.Marshal(params)
	if err != nil {
		return "{}"
	}

	return string(jsonBytes)
}
