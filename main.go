package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Constants
const (
	PacketSize       = 160 // 20ms at 8000 Hz
	PacketPeriod     = 20 * time.Millisecond
	DefaultSIPPort   = 5060
	DefaultRTPPort   = 8000
	DefaultDataPort  = 8001
	DefaultCallInt   = 5
	DefaultGPSInt    = 60
	DefaultGPSOffset = 1000
)

// Config represents the application configuration
type Config struct {
	DeviceType    int           `json:"device_type"`
	DR600Settings DR600Settings `json:"dr600_settings"`
	ARSSettings   ARSSettings   `json:"ars_settings"`
}

// DR600Settings contains DR600-specific settings
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

// ARSSettings contains ARS-specific settings
type ARSSettings struct {
	Enabled       bool `json:"enabled"`
	DefaultGroup  int  `json:"default_group"`
	SwitchDelayMs int  `json:"switch_delay_ms"`
}

// TestCase represents a test call scenario
type TestCase struct {
	SourceID int `json:"source_id"`
	GroupID  int `json:"group_id"`
	Slot     int `json:"slot"`
	FileNum  int `json:"file_num"`
}

// Repeater represents the main application
type Repeater struct {
	Config         Config
	SIPConn        *net.UDPConn
	RTPConn        *net.UDPConn
	DataConn       *net.UDPConn
	Terminals      map[int]*Terminal
	ActiveCalls    map[string]*Call
	AudioFiles     map[int][]byte
	DispatcherAddr *net.UDPAddr
	mu             sync.RWMutex
	wg             sync.WaitGroup
	shutdown       chan struct{}
	arsEnabled     bool
}

// Terminal represents a registered terminal
type Terminal struct {
	ID        int
	Addr      *net.UDPAddr
	LastSeen  time.Time
	AudioPort int
	Group     int
}

// Call represents an active call
type Call struct {
	ID           string
	GroupID      int
	Slot         int
	SourceID     int
	Participants map[int]bool
	StartTime    time.Time
	AudioFile    []byte
}

// UDPDataPacket represents a data packet sent over UDP
type UDPDataPacket struct {
	ID        int
	AudioData []byte
	Latitude  int
	Longitude int
	Timestamp time.Time
}

// RadioData represents location data for a radio
type RadioData struct {
	Timestamp     int64   `json:"timestamp"`
	SystemID      int     `json:"system_id"`
	ChannelID     int     `json:"channel_id"`
	ID            int     `json:"id"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	LatitudeType  string  `json:"latitude_type"`
	LongitudeType string  `json:"longitude_type"`
	Battery       int     `json:"battery"`
}

// LocationPacket represents a packet with radio location data
type LocationPacket struct {
	Header  [8]byte // "NDSPLOCN"
	Version uint16
	Length  uint16
	Radios  []RadioData `json:"radios"`
}

func main() {
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	repeater := NewRepeater(config)
	defer repeater.Stop()

	if err := repeater.Start(); err != nil {
		log.Fatalf("Failed to start repeater: %v", err)
	}

	log.Println("Repeater started successfully")
	select {} // Infinite wait
}

func NewRepeater(config *Config) *Repeater {
	dispatcherAddr, err := net.ResolveUDPAddr("udp",
		fmt.Sprintf("%s:%d", config.DR600Settings.DispatcherIP, config.DR600Settings.DispatcherPort))
	if err != nil {
		log.Printf("Failed to resolve dispatcher address %s:%d: %v",
			config.DR600Settings.DispatcherIP, config.DR600Settings.DispatcherPort, err)
		dispatcherAddr = nil
	} else {
		log.Printf("Dispatcher address set to %s", dispatcherAddr.String())
	}

	return &Repeater{
		Config:         *config,
		Terminals:      make(map[int]*Terminal),
		ActiveCalls:    make(map[string]*Call),
		AudioFiles:     make(map[int][]byte),
		DispatcherAddr: dispatcherAddr,
		shutdown:       make(chan struct{}),
		arsEnabled:     config.ARSSettings.Enabled,
	}
}

func loadConfig(filename string) (*Config, error) {
	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("error parsing config: %w", err)
	}

	// Set defaults for invalid values
	if config.DR600Settings.SelfIP == "" {
		config.DR600Settings.SelfIP = "0.0.0.0"
	}
	if config.DR600Settings.SIPPort <= 0 {
		config.DR600Settings.SIPPort = DefaultSIPPort
	}
	if config.DR600Settings.RTPStartPort <= 0 {
		config.DR600Settings.RTPStartPort = DefaultRTPPort
	}
	if config.DR600Settings.CallInfoInterval <= 0 {
		config.DR600Settings.CallInfoInterval = DefaultCallInt
	}
	if config.DR600Settings.GpsInfoInterval <= 0 {
		config.DR600Settings.GpsInfoInterval = DefaultGPSInt
	}
	if config.DR600Settings.GpsOffset <= 0 {
		config.DR600Settings.GpsOffset = DefaultGPSOffset
	}
	if config.ARSSettings.SwitchDelayMs <= 0 {
		config.ARSSettings.SwitchDelayMs = 500
	}

	return &config, nil
}

func (r *Repeater) Start() error {
	if err := r.loadAudioFiles(); err != nil {
		return fmt.Errorf("failed to load audio files: %w", err)
	}

	if err := r.startSIPServer(); err != nil {
		return fmt.Errorf("SIP server error: %w", err)
	}

	if err := r.startRTPServer(); err != nil {
		return fmt.Errorf("RTP server error: %w", err)
	}

	if err := r.startDataServer(); err != nil {
		return fmt.Errorf("Data server error: %w", err)
	}

	r.wg.Add(1)
	go r.runUDPServer()

	if r.Config.DR600Settings.GpsEnable {
		r.wg.Add(1)
		go r.runGPSService()
	}

	if r.Config.DR600Settings.CallsEnable {
		r.wg.Add(1)
		go r.runCallSimulator()
	}

	r.wg.Add(1)
	go r.runSIPServer()

	r.checkNetwork()

	return nil
}

func (r *Repeater) loadAudioFiles() error {
	for fileNum, filePath := range r.Config.DR600Settings.AudioFiles {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to load audio file %s: %w", filePath, err)
		}

		if strings.HasSuffix(filePath, ".wav") {
			data, err = convertWavToPCM(data)
			if err != nil {
				return fmt.Errorf("failed to convert WAV file %s: %w", filePath, err)
			}
		}

		r.AudioFiles[fileNum] = data
		log.Printf("Loaded audio file %s as file_num %d", filePath, fileNum)
	}
	return nil
}

func convertWavToPCM(wavData []byte) ([]byte, error) {
	if len(wavData) <= 44 {
		return nil, fmt.Errorf("invalid WAV file")
	}
	return wavData[44:], nil
}

func (r *Repeater) Stop() {
	close(r.shutdown)
	r.wg.Wait()

	if r.SIPConn != nil {
		r.SIPConn.Close()
	}
	if r.RTPConn != nil {
		r.RTPConn.Close()
	}
	if r.DataConn != nil {
		r.DataConn.Close()
	}
}

func (r *Repeater) startSIPServer() error {
	addr := fmt.Sprintf("%s:%d", r.Config.DR600Settings.SelfIP, r.Config.DR600Settings.SIPPort)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fallbackAddr := fmt.Sprintf("0.0.0.0:%d", r.Config.DR600Settings.SIPPort)
		log.Printf("Failed to bind to %s, trying %s", addr, fallbackAddr)

		fallbackUDPAddr, err := net.ResolveUDPAddr("udp", fallbackAddr)
		if err != nil {
			return fmt.Errorf("failed to resolve fallback address: %w", err)
		}

		conn, err = net.ListenUDP("udp", fallbackUDPAddr)
		if err != nil {
			return fmt.Errorf("failed to listen on fallback %s: %w", fallbackAddr, err)
		}
	}

	r.SIPConn = conn
	log.Printf("SIP server listening on %s", conn.LocalAddr().String())
	return nil
}

func (r *Repeater) startRTPServer() error {
	addr := fmt.Sprintf("%s:%d", r.Config.DR600Settings.SelfIP, r.Config.DR600Settings.RTPStartPort)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("resolve error: %v", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("listen error: %v", err)
	}

	r.RTPConn = conn
	log.Printf("RTP server listening on %s (UDP)", conn.LocalAddr().String())
	return nil
}

func (r *Repeater) startDataServer() error {
	dataAddr := fmt.Sprintf("%s:%d", r.Config.DR600Settings.SelfIP, DefaultDataPort)
	udpAddr, err := net.ResolveUDPAddr("udp", dataAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve data address: %w", err)
	}

	r.DataConn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to start data server: %w", err)
	}

	log.Printf("Data server listening on %s", r.DataConn.LocalAddr().String())
	return nil
}

func (r *Repeater) checkNetwork() {
	ifaces, err := net.Interfaces()
	if err != nil {
		log.Printf("Network interfaces error: %v", err)
		return
	}

	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			log.Printf("Interface %s: %s", i.Name, addr.String())
		}
	}
}

func (r *Repeater) runSIPServer() {
	defer r.wg.Done()

	buf := make([]byte, 2048)
	for {
		select {
		case <-r.shutdown:
			return
		default:
			n, addr, err := r.SIPConn.ReadFromUDP(buf)
			if err != nil {
				log.Printf("SIP read error: %v", err)
				continue
			}

			go r.handleSIPMessage(addr, buf[:n])
		}
	}
}

func (r *Repeater) handleSIPMessage(addr *net.UDPAddr, data []byte) {
	msg := string(data)
	log.Printf("Received SIP message from %s: %s", addr, string(data))

	if strings.HasPrefix(msg, "REGISTER") || strings.HasPrefix(msg, "MREGISTER") {
		r.handleRegister(addr, strings.Replace(msg, "MREGISTER", "REGISTER", 1))
	} else if strings.HasPrefix(msg, "INVITE") {
		r.handleInvite(addr, msg)
	} else if strings.HasPrefix(msg, "BYE") {
		r.handleBye(addr, msg)
	} else if strings.HasPrefix(msg, "MESSAGE") && strings.Contains(msg, "Ais-Service: ars") {
		r.handleARSMessage(addr, msg)
	}
}

func (r *Repeater) handleARSMessage(addr *net.UDPAddr, msg string) {
	// Implementation for ARS message handling
	log.Printf("Received ARS message from %s: %s", addr, msg)
}

func (r *Repeater) handleRegister(addr *net.UDPAddr, msg string) {
	headers := parseSIPHeaders(msg)

	from := headers["From"]
	to := headers["To"]
	callID := headers["Call-ID"]
	cSeq := headers["CSeq"]
	contact := headers["Contact"]
	expires := headers["Expires"]
	via := headers["Via"]

	var terminalID int
	if strings.Contains(from, "<") {
		fmt.Sscanf(from, "<sip:%d@", &terminalID)
	} else {
		fmt.Sscanf(from, "sip:%d@", &terminalID)
	}

	r.mu.Lock()
	r.Terminals[terminalID] = &Terminal{
		ID:       terminalID,
		Addr:     addr,
		LastSeen: time.Now(),
		Group:    r.Config.ARSSettings.DefaultGroup,
	}
	r.mu.Unlock()

	response := fmt.Sprintf(
		"SIP/2.0 200 OK\r\n"+
			"Via: %s\r\n"+
			"From: %s\r\n"+
			"To: %s;tag=%s\r\n"+
			"Call-ID: %s\r\n"+
			"CSeq: %s\r\n"+
			"Contact: %s\r\n"+
			"Expires: %s\r\n"+
			"Content-Length: 0\r\n\r\n",
		via,
		from,
		to,
		generateTag(),
		callID,
		cSeq,
		contact,
		expires,
	)

	if _, err := r.SIPConn.WriteToUDP([]byte(response), addr); err != nil {
		log.Printf("Failed to send REGISTER response: %v", err)
	} else {
		log.Printf("Registered terminal %d from %s", terminalID, addr)
		if r.DispatcherAddr != nil {
			r.sendToDispatcher(fmt.Sprintf("TERMINAL_REG %d %s\n", terminalID, addr))
		}
	}
}

func (r *Repeater) handleInvite(addr *net.UDPAddr, msg string) {
	headers := parseSIPHeaders(msg)

	from := headers["From"]
	to := headers["To"]
	callID := headers["Call-ID"]
	cSeq := headers["CSeq"]
	via := headers["Via"]

	var sourceID, groupID int
	fmt.Sscanf(from, "<sip:%d@", &sourceID)
	fmt.Sscanf(to, "<sip:%d@", &groupID)

	slot := 1
	if r.arsEnabled {
		r.mu.RLock()
		if term, exists := r.Terminals[sourceID]; exists {
			groupID = term.Group
		}
		r.mu.RUnlock()
	}

	call := &Call{
		ID:           callID,
		GroupID:      groupID,
		SourceID:     sourceID,
		Slot:         slot,
		Participants: make(map[int]bool),
		StartTime:    time.Now(),
	}

	r.mu.Lock()
	for id, term := range r.Terminals {
		if id != sourceID {
			if !r.arsEnabled || term.Group == groupID {
				call.Participants[id] = true
			}
		}
	}
	r.ActiveCalls[callID] = call
	r.mu.Unlock()

	response := fmt.Sprintf(
		"SIP/2.0 200 OK\r\n"+
			"Via: %s\r\n"+
			"From: %s\r\n"+
			"To: %s;tag=%s\r\n"+
			"Call-ID: %s\r\n"+
			"CSeq: %s\r\n"+
			"Content-Type: application/sdp\r\n"+
			"Content-Length: %d\r\n\r\n"+
			"v=0\r\n"+
			"o=- 0 0 IN IP4 %s\r\n"+
			"s=Talk\r\n"+
			"c=IN IP4 %s\r\n"+
			"t=0 0\r\n"+
			"m=audio %d RTP/AVP 0\r\n",
		via,
		from,
		to,
		generateTag(),
		callID,
		cSeq,
		100,
		r.Config.DR600Settings.SelfIP,
		r.Config.DR600Settings.SelfIP,
		r.Config.DR600Settings.RTPStartPort,
	)

	if _, err := r.SIPConn.WriteToUDP([]byte(response), addr); err != nil {
		log.Printf("Failed to send INVITE response: %v", err)
	} else {
		log.Printf("Accepted call %s from %d to group %d", callID, sourceID, groupID)
		r.sendCallUpdate(call)
		go r.startAudioStream(call)
	}
}

func (r *Repeater) handleBye(addr *net.UDPAddr, msg string) {
	headers := parseSIPHeaders(msg)
	callID := headers["Call-ID"]
	via := headers["Via"]
	from := headers["From"]
	to := headers["To"]
	cSeq := headers["CSeq"]

	r.mu.Lock()
	delete(r.ActiveCalls, callID)
	r.mu.Unlock()

	response := fmt.Sprintf(
		"SIP/2.0 200 OK\r\n"+
			"Via: %s\r\n"+
			"From: %s\r\n"+
			"To: %s\r\n"+
			"Call-ID: %s\r\n"+
			"CSeq: %s\r\n"+
			"Content-Length: 0\r\n\r\n",
		via,
		from,
		to,
		callID,
		cSeq,
	)

	if _, err := r.SIPConn.WriteToUDP([]byte(response), addr); err != nil {
		log.Printf("Failed to send BYE response: %v", err)
	}

	log.Printf("Call %s terminated", callID)
	r.sendToDispatcher(fmt.Sprintf("CALL_END %s\n", callID))
}

func (r *Repeater) sendCallUpdate(call *Call) {
	if r.DispatcherAddr == nil {
		return
	}

	msg := fmt.Sprintf("CALL_UPDATE %s %d %d %d %v %d\n",
		call.ID,
		call.SourceID,
		call.GroupID,
		call.Slot,
		call.StartTime,
		len(call.Participants))
	r.sendToDispatcher(msg)
}

func (r *Repeater) sendToDispatcher(data string) {
	if r.DispatcherAddr == nil {
		log.Println("Dispatcher address not configured")
		return
	}

	log.Printf("Sending to dispatcher %s: %s", r.DispatcherAddr.String(), strings.TrimSpace(data))
	if _, err := r.SIPConn.WriteToUDP([]byte(data), r.DispatcherAddr); err != nil {
		log.Printf("Dispatcher send error: %v", err)
	}
}

func (r *Repeater) startAudioStream(call *Call) {
	callInfoInterval := time.Duration(r.Config.DR600Settings.CallInfoInterval) * time.Second
	if callInfoInterval <= 0 {
		callInfoInterval = 5 * time.Second
	}

	r.mu.RLock()
	audioData := call.AudioFile
	if len(audioData) == 0 {
		audioData = r.AudioFiles[call.SourceID%3+1]
	}
	r.mu.RUnlock()

	if len(audioData) == 0 {
		audioData = generateDefaultAudio()
	}

	ticker := time.NewTicker(PacketPeriod)
	defer ticker.Stop()

	infoTicker := time.NewTicker(callInfoInterval)
	defer infoTicker.Stop()

	for offset := 0; offset < len(audioData); offset += PacketSize {
		select {
		case <-ticker.C:
			end := offset + PacketSize
			if end > len(audioData) {
				end = len(audioData)
			}

			packet := audioData[offset:end]
			r.broadcastAudio(call, packet)

		case <-infoTicker.C:
			r.sendCallUpdate(call)

		case <-r.shutdown:
			return
		}
	}

	r.mu.Lock()
	delete(r.ActiveCalls, call.ID)
	r.mu.Unlock()

	r.sendToDispatcher(fmt.Sprintf("CALL_END %s\n", call.ID))
}

func (r *Repeater) broadcastAudio(call *Call, audio []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	lat := r.Config.DR600Settings.Latitude + rand.Intn(100) - 50
	lon := r.Config.DR600Settings.Longitude + rand.Intn(100) - 50

	for participantID := range call.Participants {
		if term, exists := r.Terminals[participantID]; exists {
			// Send RTP audio
			rtpAddr := &net.UDPAddr{
				IP:   term.Addr.IP,
				Port: r.Config.DR600Settings.RTPStartPort,
			}
			if _, err := r.RTPConn.WriteToUDP(audio, rtpAddr); err != nil {
				log.Printf("RTP send error to %d: %v", participantID, err)
			}

			// Send structured data
			packet := UDPDataPacket{
				ID:        call.SourceID,
				AudioData: audio,
				Latitude:  lat,
				Longitude: lon,
				Timestamp: time.Now(),
			}
			data, err := packet.Marshal()
			if err != nil {
				log.Printf("Marshal error: %v", err)
				continue
			}

			dataAddr := &net.UDPAddr{
				IP:   term.Addr.IP,
				Port: DefaultDataPort,
			}
			if _, err = r.DataConn.WriteToUDP(data, dataAddr); err != nil {
				log.Printf("Data send error to %d: %v", participantID, err)
			}
		}
	}
}

func (r *Repeater) runUDPServer() {
	defer r.wg.Done()
	buf := make([]byte, 1500) // MTU size

	for {
		select {
		case <-r.shutdown:
			return
		default:
			n, addr, err := r.DataConn.ReadFromUDP(buf)
			if err != nil {
				log.Printf("UDP read error: %v", err)
				continue
			}

			log.Printf("Received %d bytes from %s: %x", n, addr, buf[:n])

			packet, err := UnmarshalUDPData(buf[:n])
			if err != nil {
				log.Printf("Unmarshal error: %v", err)
				continue
			}

			log.Printf("Decoded packet from %s: ID=%d, Lat=%d, Lon=%d",
				addr, packet.ID, packet.Latitude, packet.Longitude)
			r.processUDPPacket(packet, addr)
		}
	}
}

func (r *Repeater) processUDPPacket(packet *UDPDataPacket, addr *net.UDPAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term, exists := r.Terminals[packet.ID]; exists {
		term.LastSeen = time.Now()
		term.Addr = addr
	} else {
		r.Terminals[packet.ID] = &Terminal{
			ID:       packet.ID,
			Addr:     addr,
			LastSeen: time.Now(),
			Group:    r.Config.ARSSettings.DefaultGroup,
		}
		log.Printf("New terminal registered via UDP: %d", packet.ID)
	}
}

func (r *Repeater) runGPSService() {
	defer r.wg.Done()

	if !r.Config.DR600Settings.GpsEnable {
		return
	}

	gpsOffset := time.Duration(r.Config.DR600Settings.GpsOffset) * time.Millisecond
	if gpsOffset <= 0 {
		gpsOffset = DefaultGPSOffset * time.Millisecond
	}

	gpsInfoInterval := time.Duration(r.Config.DR600Settings.GpsInfoInterval) * time.Second
	if gpsInfoInterval <= 0 {
		gpsInfoInterval = DefaultGPSInt * time.Second
	}

	ticker := time.NewTicker(gpsOffset)
	defer ticker.Stop()

	infoTicker := time.NewTicker(gpsInfoInterval)
	defer infoTicker.Stop()

	for {
		select {
		case <-r.shutdown:
			return
		case <-ticker.C:
			r.sendGPSUpdatesToTerminals()
		case <-infoTicker.C:
			if err := r.sendRadioData(); err != nil {
				log.Printf("Error sending radio data: %v", err)
			}
		}
	}
}

func (r *Repeater) sendGPSUpdatesToTerminals() {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, radioID := range r.Config.DR600Settings.Radios {
		if term, exists := r.Terminals[radioID]; exists {
			newLat := r.Config.DR600Settings.Latitude + rand.Intn(100) - 50
			newLon := r.Config.DR600Settings.Longitude + rand.Intn(100) - 50

			lipMsg := fmt.Sprintf(
				"MESSAGE sip:%d@%s SIP/2.0\r\n"+
					"Ais-Service: location\r\n"+
					"Ais-Msg-id: location-info; longitude=%d; latitude=%d; position error=1; speed=50\r\n"+
					"Content-Length: 0\r\n\r\n",
				radioID,
				r.Config.DR600Settings.SelfIP,
				newLon,
				newLat,
			)

			if _, err := r.SIPConn.WriteToUDP([]byte(lipMsg), term.Addr); err != nil {
				log.Printf("Failed to send GPS to %d: %v", radioID, err)
			}
		}
	}
}

func (r *Repeater) runCallSimulator() {
	defer r.wg.Done()

	if !r.Config.DR600Settings.CallsEnable {
		return
	}

	for _, testCase := range r.Config.DR600Settings.TestCases {
		r.wg.Add(1)
		go func(tc TestCase) {
			defer r.wg.Done()

			delay := time.Duration(r.Config.DR600Settings.MinDelay+
				rand.Intn(r.Config.DR600Settings.MaxDelay-r.Config.DR600Settings.MinDelay)) *
				time.Second

			select {
			case <-time.After(delay):
				r.initiateTestCall(tc.SourceID, tc.GroupID, tc.Slot, tc.FileNum)
			case <-r.shutdown:
				return
			}
		}(testCase)
	}
}

func (r *Repeater) initiateTestCall(sourceID, groupID, slot, fileNum int) {
	r.mu.RLock()
	audioData, exists := r.AudioFiles[fileNum]
	r.mu.RUnlock()

	if !exists {
		log.Printf("Audio file %d not found, using default", fileNum)
		audioData = generateDefaultAudio()
	}

	callID := fmt.Sprintf("%d-%d-%d", sourceID, groupID, time.Now().Unix())

	call := &Call{
		ID:           callID,
		GroupID:      groupID,
		Slot:         slot,
		SourceID:     sourceID,
		Participants: make(map[int]bool),
		StartTime:    time.Now(),
		AudioFile:    audioData,
	}

	r.mu.Lock()
	for id, term := range r.Terminals {
		if id != sourceID {
			if !r.arsEnabled || term.Group == groupID {
				call.Participants[id] = true
			}
		}
	}
	r.ActiveCalls[callID] = call
	r.mu.Unlock()

	log.Printf("Initiated test call %s from %d to group %d", callID, sourceID, groupID)
	r.sendCallUpdate(call)
	go r.startAudioStream(call)
}

func parseSIPHeaders(msg string) map[string]string {
	headers := make(map[string]string)
	lines := strings.Split(msg, "\r\n")

	for _, line := range lines {
		if idx := strings.Index(line, ":"); idx > 0 {
			key := strings.TrimSpace(line[:idx])
			value := strings.TrimSpace(line[idx+1:])
			key = strings.Title(strings.ToLower(key))
			headers[key] = value
		}
	}

	return headers
}

func generateTag() string {
	return fmt.Sprintf("%x", rand.Uint32())
}

func generateDefaultAudio() []byte {
	const (
		sampleRate = 8000
		duration   = 2 * time.Second
		freq       = 440 // Hz
	)

	samples := int(sampleRate * duration.Seconds())
	data := make([]byte, samples)

	for i := 0; i < samples; i++ {
		val := math.Sin(2 * math.Pi * freq * float64(i) / float64(sampleRate))
		data[i] = byte(127 * (1 + val))
	}

	return data
}

func (p *UDPDataPacket) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(p)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalUDPData(data []byte) (*UDPDataPacket, error) {
	var packet UDPDataPacket
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&packet)
	if err != nil {
		return nil, err
	}
	return &packet, nil
}

func NewLocationPacket(radios []RadioData) *LocationPacket {
	return &LocationPacket{
		Header:  [8]byte{'N', 'D', 'S', 'P', 'L', 'O', 'C', 'N'},
		Version: 0x0594,
		Radios:  radios,
	}
}

func (p *LocationPacket) Marshal() ([]byte, error) {
	jsonData, err := json.Marshal(struct {
		Radios []RadioData `json:"radios"`
	}{p.Radios})
	if err != nil {
		return nil, err
	}

	p.Length = uint16(len(jsonData))

	buf := new(bytes.Buffer)
	buf.Write(p.Header[:])
	binary.Write(buf, binary.LittleEndian, p.Version)
	binary.Write(buf, binary.LittleEndian, p.Length)
	buf.Write(jsonData)

	return buf.Bytes(), nil
}

func (r *Repeater) sendRadioData() error {
	var radios []RadioData
	now := time.Now().Unix()

	r.mu.RLock()
	for _, radioID := range r.Config.DR600Settings.Radios {
		lat := float64(r.Config.DR600Settings.Latitude)/100000.0 + rand.Float64()*0.01 - 0.005
		lon := float64(r.Config.DR600Settings.Longitude)/100000.0 + rand.Float64()*0.01 - 0.005

		radios = append(radios, RadioData{
			Timestamp:     now,
			SystemID:      1,
			ChannelID:     0,
			ID:           radioID,
			Latitude:     lat,
			Longitude:    lon,
			LatitudeType:  "N",
			LongitudeType: "E",
			Battery:      -1,
		})
	}
	r.mu.RUnlock()

	packet := NewLocationPacket(radios)
	packetData, err := packet.Marshal()
	if err != nil {
		return fmt.Errorf("marshal error: %v", err)
	}

	serverAddr := fmt.Sprintf("%s:%d", r.Config.DR600Settings.ServerIP, r.Config.DR600Settings.ServerPort)
	udpAddr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return fmt.Errorf("resolve error: %v", err)
	}

	_, err = r.DataConn.WriteToUDP(packetData, udpAddr)
	if err != nil {
		return fmt.Errorf("send error: %v", err)
	}

	log.Printf("Sent location packet to %s (%d bytes)", serverAddr, len(packetData))
	return nil
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[DR600] ")
}