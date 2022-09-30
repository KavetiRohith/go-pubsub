package main

import (
	"encoding/json"
	"io"
	"os"
	"strings"
)

type SubscriptionsMap map[string]map[string]struct{}

// MarshalJSON creates a JSON array from the set (map[string]struct{})
func (s SubscriptionsMap) MarshalJSON() ([]byte, error) {
	finalMap := make(map[string][]string)

	for key, set := range s {
		values := make([]string, 0, len(set))

		for value := range set {
			values = append(values, value)
		}

		finalMap[key] = values
	}

	return json.Marshal(finalMap)
}

// UnmarshalJSON recreates a set (map[string]struct{}) from a JSON array
func (s *SubscriptionsMap) UnmarshalJSON(b []byte) error {
	subsMap := make(map[string][]string)

	err := json.Unmarshal(b, &subsMap)
	if err != nil {
		return err
	}

	for key, valueList := range subsMap {
		subsSet := make(map[string]struct{}, len(valueList))

		for _, value := range valueList {
			subsSet[value] = struct{}{}
		}

		(*s)[key] = subsSet
	}

	return nil
}

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
