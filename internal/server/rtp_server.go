package main

import (
	"fmt"
	"log"
	"net"
    "retroemul/service"
)

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
        
        fallbackUDPAddr, _ := net.ResolveUDPAddr("udp", fallbackAddr)
        conn, err = net.ListenUDP("udp", fallbackUDPAddr)
        if err != nil {
            return fmt.Errorf("failed to listen on fallback %s: %w", fallbackAddr, err)
        }
    }

    r.RTPConn = conn
    log.Printf("RTP server listening on %s", conn.LocalAddr().String())
    return nil
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