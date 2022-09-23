package main

import (
	"encoding/json"
	"io"
	"os"
	"strings"
)

type SubscriptionsMap map[string][]string

func ReadSubscriptionsFromFile(path string) (SubscriptionsMap, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	result := make(SubscriptionsMap)

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(byteValue))) == 0 {
		return result, nil
	}

	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
