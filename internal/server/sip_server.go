package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
	"retroemul/internal/models"
)

func (r *Repeater) startSIPServer() error {
	addr := fmt.Sprintf("%s:%d", r.Config.DR600Settings.SelfIP, r.Config.DR600Settings.SIPPort)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("resolve SIP address error: %w", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fallbackAddr := fmt.Sprintf("0.0.0.0:%d", r.Config.DR600Settings.SIPPort)
		log.Printf("Failed to bind to %s, trying %s", addr, fallbackAddr)

		fallbackUDPAddr, _ := net.ResolveUDPAddr("udp", fallbackAddr)
		conn, err = net.ListenUDP("udp", fallbackUDPAddr)
		if err != nil {
			return fmt.Errorf("fallback SIP listen error: %w", err)
		}
	}

	r.SIPConn = conn
	log.Printf("SIP server started on %s", conn.LocalAddr().String())
	return nil
}

func (r *Repeater) runSIPServer() {
	defer r.wg.Done()
	buffer := make([]byte, 4096)

	for {
		select {
		case <-r.shutdown:
			return
		default:
			n, addr, err := r.SIPConn.ReadFromUDP(buffer)
			if err != nil {
				if !strings.Contains(err.Error(), "closed") {
					log.Printf("SIP read error: %v", err)
				}
				continue
			}

			msg := bytes.Trim(buffer[:n], "\x00")
			go r.processSIPMessage(addr, msg)
		}
	}
}

func (r *Repeater) processSIPMessage(addr *net.UDPAddr, data []byte) {
	msg := string(data)
	log.Printf("SIP message from %s:\n%s", addr, msg)

	switch {
	case strings.HasPrefix(msg, "REGISTER"):
		r.handleRegistration(addr, msg)
	case strings.HasPrefix(msg, "INVITE"):
		r.handleInvite(addr, msg)
	case strings.HasPrefix(msg, "BYE"):
		r.handleTermination(addr, msg)
	case strings.HasPrefix(msg, "ARS"):
		r.handleARSPacket(addr, msg)
	default:
		log.Printf("Unknown SIP message type: %s", msg[:32])
	}
}

func (r *Repeater) handleRegistration(addr *net.UDPAddr, msg string) {
	headers := parseSIPHeaders(msg)
	terminalID := extractTerminalID(headers["From"])

	r.mu.Lock()
	defer r.mu.Unlock()

	// Обновление информации о терминале
	r.Terminals[terminalID] = &Terminal{
		ID:       terminalID,
		Addr:     addr,
		LastSeen: time.Now(),
	}

	// Формирование ответа
	response := buildSIPResponse(
		headers["Via"],
		headers["From"],
		headers["To"],
		headers["Call-ID"],
		headers["CSeq"],
	)

	if _, err := r.SIPConn.WriteToUDP([]byte(response), addr); err != nil {
		log.Printf("Registration response error: %v", err)
	} else {
		log.Printf("Terminal %d registered", terminalID)
		r.sendToDispatcher(fmt.Sprintf("TERMINAL_ONLINE %d", terminalID))
	}
}

func (r *Repeater) handleInvite(addr *net.UDPAddr, msg string) {
	headers := parseSIPHeaders(msg)
	callID := headers["Call-ID"]
	sourceID := extractTerminalID(headers["From"])
	groupID := extractTerminalID(headers["To"])

	// Создание вызова
	call := &Call{
		ID:           callID,
		SourceID:     sourceID,
		GroupID:      groupID,
		StartTime:    time.Now(),
		Participants: make(map[int]bool),
	}

	r.mu.Lock()
	// Добавление участников
	for id := range r.Terminals {
		if id != sourceID {
			call.Participants[id] = true
		}
	}
	r.ActiveCalls[callID] = call
	r.mu.Unlock()

	// Формирование SDP ответа
	sdp := fmt.Sprintf(
		"v=0\r\no=- 0 0 IN IP4 %s\r\ns=Conference\r\nc=IN IP4 %s\r\nt=0 0\r\nm=audio %d RTP/AVP 0\r\n",
		r.Config.DR600Settings.SelfIP,
		r.Config.DR600Settings.SelfIP,
		r.Config.DR600Settings.RTPStartPort,
	)

	response := buildSIPResponse(
		headers["Via"],
		headers["From"],
		headers["To"],
		callID,
		headers["CSeq"],
	) + fmt.Sprintf("Content-Type: application/sdp\r\nContent-Length: %d\r\n\r\n%s", len(sdp), sdp)

	if _, err := r.SIPConn.WriteToUDP([]byte(response), addr); err != nil {
		log.Printf("INVITE response error: %v", err)
	} else {
		log.Printf("Call %s established (Source: %d → Group: %d)", callID, sourceID, groupID)
		go r.startAudioStream(call)
	}
}

func (r *Repeater) handleTermination(addr *net.UDPAddr, msg string) {
	headers := parseSIPHeaders(msg)
	callID := headers["Call-ID"]

	r.mu.Lock()
	delete(r.ActiveCalls, callID)
	r.mu.Unlock()

	response := buildSIPResponse(
		headers["Via"],
		headers["From"],
		headers["To"],
		callID,
		headers["CSeq"],
	)

	if _, err := r.SIPConn.WriteToUDP([]byte(response), addr); err != nil {
		log.Printf("BYE response error: %v", err)
	} else {
		log.Printf("Call %s terminated", callID)
	}
}

func (r *Repeater) handleARSPacket(addr *net.UDPAddr, msg string) {
	headers := parseSIPHeaders(msg)
	sequence := headers["Sequence"]

	response := fmt.Sprintf(
		"ARS/1.0 200 OK\r\n"+
			"Sequence: %s\r\n"+
			"Timestamp: %d\r\n\r\n",
		sequence,
		time.Now().Unix(),
	)

	if _, err := r.SIPConn.WriteToUDP([]byte(response), addr); err != nil {
		log.Printf("ARS response error: %v", err)
	} else {
		log.Printf("ARS request processed: %s", sequence)
	}
}

// Вспомогательные функции
func buildSIPResponse(via, from, to, callID, cseq string) string {
	return fmt.Sprintf(
		"SIP/2.0 200 OK\r\n"+
			"Via: %s\r\n"+
			"From: %s\r\n"+
			"To: %s;tag=%s\r\n"+
			"Call-ID: %s\r\n"+
			"CSeq: %s\r\n",
		via,
		from,
		to,
		generateTag(),
		callID,
		cseq,
	)
}

func extractTerminalID(s string) int {
	var id int
	fmt.Sscanf(s, "<sip:%d@", &id)
	return id
}