package main

import (
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

type Config struct {
	DeviceType    int `json:"device_type"`
	DR600Settings struct {
		SelfID            int            `json:"self_id"`
		SelfName          string         `json:"self_name"`
		SelfIP            string         `json:"self_ip"`
		SIPPort           int            `json:"sip_port"`
		SIPServerName     string         `json:"sip_server_name"`
		LocalName         string         `json:"sip_local_name"`
		RTPStartPort      int            `json:"rtp_start_port"`
		ServerIP          string         `json:"server_ip"`
		ServerPort        int            `json:"server_port"`
		Login             string         `json:"login"`
		Password          string         `json:"password"`
		MinDelay          int            `json:"min_delay_s"`
		MaxDelay          int            `json:"max_delay_s"`
		DispatcherIP      string         `json:"dispatcher_ip"`
		DispatcherPort    int            `json:"dispatcher_port"`
		CallInfoInterval  int            `json:"call_info_interval"`
		GpsInfoInterval   int            `json:"gps_info_interval"`
		TestCases         []TestCase     `json:"test_cases"`
		Radios            []int          `json:"Radios"`
		Latitude          int            `json:"latitude"`
		Longitude         int            `json:"longitude"`
		GpsOffset         int            `json:"gps_offset"`
		CallsEnable       bool           `json:"calls_enable"`
		GpsEnable         bool           `json:"gps_enable"`
		AudioFiles        map[int]string `json:"audio_files"`
	} `json:"dr600_settings"`
}

type TestCase struct {
	SourceID int `json:"source_id"`
	GroupID  int `json:"group_id"`
	Slot     int `json:"slot"`
	FileNum  int `json:"file_num"`
}

type Repeater struct {
	Config         Config
	SIPConn        *net.UDPConn
	RTPConn        *net.UDPConn
	Terminals      map[int]*Terminal
	ActiveCalls    map[string]*Call
	AudioFiles     map[int][]byte
	DispatcherAddr *net.UDPAddr
	mu             sync.RWMutex
	wg             sync.WaitGroup
	shutdown       chan struct{}
}

type Terminal struct {
	ID        int
	Addr      *net.UDPAddr
	LastSeen  time.Time
	AudioPort int
}

type Call struct {
	ID           string
	GroupID      int
	Slot         int
	SourceID     int
	Participants map[int]bool
	StartTime    time.Time
	AudioFile    []byte
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
	dispatcherAddr, _ := net.ResolveUDPAddr("udp", 
		fmt.Sprintf("%s:%d", config.DR600Settings.DispatcherIP, config.DR600Settings.DispatcherPort))

	return &Repeater{
		Config:         *config,
		Terminals:      make(map[int]*Terminal),
		ActiveCalls:    make(map[string]*Call),
		AudioFiles:     make(map[int][]byte),
		DispatcherAddr: dispatcherAddr,
		shutdown:       make(chan struct{}),
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

	if config.DR600Settings.SelfIP == "" {
		config.DR600Settings.SelfIP = "0.0.0.0"
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

func (r *Repeater) checkNetwork() {
	if r.DispatcherAddr == nil {
		return
	}

	conn, err := net.Dial("udp", r.DispatcherAddr.String())
	if err != nil {
		log.Printf("Dispatcher connection error: %v", err)
	} else {
		conn.Close()
		log.Println("Dispatcher connection OK")
	}
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
		return err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fallbackAddr := fmt.Sprintf("0.0.0.0:%d", r.Config.DR600Settings.RTPStartPort)
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

	r.RTPConn = conn
	log.Printf("RTP server listening on %s", conn.LocalAddr().String())
	return nil
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
	log.Printf("Received message from %s: %s", addr, string(data))
	
	if strings.HasPrefix(msg, "REGISTER") {
		r.handleRegister(addr, msg)
	} else if strings.HasPrefix(msg, "INVITE") {
		r.handleInvite(addr, msg)
	} else if strings.HasPrefix(msg, "BYE") {
		r.handleBye(addr, msg)
	}
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
	fmt.Sscanf(from, "<sip:%d@", &terminalID)

	r.mu.Lock()
	r.Terminals[terminalID] = &Terminal{
		ID:       terminalID,
		Addr:     addr,
		LastSeen: time.Now(),
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
		r.sendToDispatcher(fmt.Sprintf("TERMINAL_REG %d %s\n", terminalID, addr))
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

	call := &Call{
		ID:           callID,
		GroupID:      groupID,
		SourceID:     sourceID,
		Participants: make(map[int]bool),
		StartTime:    time.Now(),
	}

	r.mu.Lock()
	for id, term := range r.Terminals {
		if id != sourceID {
			call.Participants[id] = true
			_=term
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

func (r *Repeater) sendCallUpdate(call *Call) {
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
		return
	}

	if _, err := r.SIPConn.WriteToUDP([]byte(data), r.DispatcherAddr); err != nil {
		log.Printf("Dispatcher send error: %v", err)
	}
}

func (r *Repeater) startAudioStream(call *Call) {
	const (
		packetSize   = 160 // 20ms at 8000 Hz
		packetPeriod = 20 * time.Millisecond
	)

	r.mu.RLock()
	audioData, exists := r.AudioFiles[call.SourceID%3+1]
	r.mu.RUnlock()

	if !exists {
		audioData = generateDefaultAudio()
	}

	ticker := time.NewTicker(packetPeriod)
	defer ticker.Stop()

	infoTicker := time.NewTicker(time.Duration(r.Config.DR600Settings.CallInfoInterval) * time.Second)
	defer infoTicker.Stop()

	for offset := 0; offset < len(audioData); offset += packetSize {
		select {
		case <-ticker.C:
			end := offset + packetSize
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

	for participantID := range call.Participants {
		if term, exists := r.Terminals[participantID]; exists {
			_, err := r.RTPConn.WriteToUDP(audio, term.Addr)
			if err != nil {
				log.Printf("Error sending audio to %d: %v", participantID, err)
			}
		}
	}
}

func (r *Repeater) handleBye(addr *net.UDPAddr, msg string) {
	headers := parseSIPHeaders(msg)
	callID := headers["Call-ID"]

	r.mu.Lock()
	delete(r.ActiveCalls, callID)
	r.mu.Unlock()

	log.Printf("Call %s terminated", callID)
	r.sendToDispatcher(fmt.Sprintf("CALL_END %s\n", callID))
}

func (r *Repeater) runGPSService() {
    defer r.wg.Done()

    if !r.Config.DR600Settings.GpsEnable {
        return
    }

    // Ensure GpsOffset is at least 1 millisecond
    gpsOffset := time.Duration(r.Config.DR600Settings.GpsOffset) * time.Millisecond
    if gpsOffset <= 0 {
        gpsOffset = 1000 * time.Millisecond // Default to 1 second if invalid
    }

    ticker := time.NewTicker(gpsOffset)
    defer ticker.Stop()

    // Ensure GpsInfoInterval is at least 1 second
    gpsInfoInterval := time.Duration(r.Config.DR600Settings.GpsInfoInterval) * time.Second
    if gpsInfoInterval <= 0 {
        gpsInfoInterval = 60 * time.Second // Default to 1 minute if invalid
    }

    infoTicker := time.NewTicker(gpsInfoInterval)
    defer infoTicker.Stop()

    for {
        select {
        case <-r.shutdown:
            return
        case <-ticker.C:
            r.mu.RLock()
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
            r.mu.RUnlock()

        case <-infoTicker.C:
            r.mu.RLock()
            for _, radioID := range r.Config.DR600Settings.Radios {
                if term, exists := r.Terminals[radioID]; exists {
                    newLat := r.Config.DR600Settings.Latitude + rand.Intn(100) - 50
                    newLon := r.Config.DR600Settings.Longitude + rand.Intn(100) - 50
					_=term

                    gpsMsg := fmt.Sprintf("GPS_UPDATE %d %d %d %s\n",
                        radioID, newLat, newLon, time.Now().Format(time.RFC3339))
                    r.sendToDispatcher(gpsMsg)
                }
            }
            r.mu.RUnlock()
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
			call.Participants[id] = true
			_=term
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