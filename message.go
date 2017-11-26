package main

import "time"

type message struct {
	Key       interface{} `json:"key,omitempty"`
	Value     interface{} `json:"value,omitempty"`
	Offset    *int64      `json:"offset,omitempty"`
	Timestamp *time.Time  `json:"timestamp,omitempty"`
}

func (m *message) reset() {
	m.Key = nil
	m.Value = nil
	m.Offset = nil
	m.Timestamp = nil
}

func (m *message) addKey(key []byte, isBinary bool) {
	if isBinary {
		m.Key = key
	} else {
		m.Key = string(key)
	}
}

func (m *message) addValue(value []byte, isBinary bool) {
	if isBinary {
		m.Value = value
	} else {
		m.Value = string(value)
	}
}

func (m *message) addOffset(offset int64) {
	m.Offset = &offset
}

func (m *message) addTimestamp(timestamp time.Time) {
	m.Timestamp = &timestamp
}
