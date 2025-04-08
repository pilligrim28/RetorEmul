package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type DR600Settings struct {
	SelfID           int            `json:"self_id"`
	SelfName         string         `json:"self_name"`
	SelfIP           string         `json:"self_ip"`
	SIPPort          int            `json:"sip_port"`
	SIPServerName    string         `json:"sip_server_name"`
	LocalName        string         `json:"sip_local_name"`
	RTPStartPort     int            `json:"rtp_start_port"`
	ServerIP         string         `json:"server_ip"`
	ServerPort       int            `json:"server_port"`
	Login            string         `json:"login"`
	Password         string         `json:"password"`
	MinDelay         int            `json:"min_delay_s"`
	MaxDelay         int            `json:"max_delay_s"`
	DispatcherIP     string         `json:"dispatcher_ip"`
	DispatcherPort   int            `json:"dispatcher_port"`
	CallInfoInterval int            `json:"call_info_interval"`
	GpsInfoInterval  int            `json:"gps_info_interval"`
	TestCases        []TestCase     `json:"test_cases"`
	Radios           []int          `json:"Radios"`
	Latitude         int            `json:"latitude"`
	Longitude        int            `json:"longitude"`
	GpsOffset        int            `json:"gps_offset"`
	CallsEnable      bool           `json:"calls_enable"`
	GpsEnable        bool           `json:"gps_enable"`
	AudioFiles       map[int]string `json:"audio_files"`
}
type Config struct {
	DeviceType    int           `json:"device_type"`
	DR600Settings DR600Settings `json:"dr600_settings"`
}

type TestCase struct {
	SourceID int `json:"source_id"`
	GroupID  int `json:"group_id"`
	Slot     int `json:"slot"`
	FileNum  int `json:"file_num"`
}

func LoadConfig(filename string) (*Config, error) {
	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("error parsing config: %w", err)
	}

	if config.DR600Settings.SelfIP == "" {
		config.DR600Settings.SelfIP = "0.0.0.0"
	}
	if config.DR600Settings.CallInfoInterval <= 0 {
		config.DR600Settings.CallInfoInterval = 5
	}
	if config.DR600Settings.GpsInfoInterval <= 0 {
		config.DR600Settings.GpsInfoInterval = 60
	}
	if config.DR600Settings.GpsOffset <= 0 {
		config.DR600Settings.GpsOffset = 1000
	}

	return &config, nil
}
