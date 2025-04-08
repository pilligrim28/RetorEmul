package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

// Обработка входящих ARS пакетов
// func (r *Repeater) handleARSPacket(addr *net.UDPAddr, msg string) {
//     headers := parseSIPHeaders(msg)
//     sequence := headers["Sequence"]
    
//     response := fmt.Sprintf(
//         "ARS/1.0 200 OK\r\n"+
//         "Sequence: %s\r\n"+
//         "Timestamp: %d\r\n\r\n",
//         sequence,
//         time.Now().Unix(),
//     )
    
//     if _, err := r.SIPConn.WriteToUDP([]byte(response), addr); err != nil {
//         log.Printf("Failed to send ARS response: %v", err)
//     }
// }

// Отправка ARS пакетов
func (r *Repeater) SendARSPacket(dest *net.UDPAddr) {
    sequence := fmt.Sprintf("%d", time.Now().UnixNano())
    packet := fmt.Sprintf(
        "ARS/1.0 REQUEST\r\n"+
        "Sequence: %s\r\n"+
        "Timestamp: %d\r\n\r\n",
        sequence,
        time.Now().Unix(),
    )

    if _, err := r.SIPConn.WriteToUDP([]byte(packet), dest); err != nil {
        log.Printf("ARS send error: %v", err)
    }
}