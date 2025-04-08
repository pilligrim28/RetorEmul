package main

import (
    "fmt"
    "math/rand"
    "time"
	
)

func (r *Repeater) runGPSService() {
    defer r.wg.Done()
    if !r.Config.DR600Settings.GpsEnable {
        return
    }

    ticker := time.NewTicker(time.Duration(r.Config.DR600Settings.GpsOffset) * time.Millisecond)
    defer ticker.Stop()

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
                        "Ais-Msg-id: location-info; longitude=%d; latitude=%d\r\n"+
                        "Content-Length: 0\r\n\r\n",
                        radioID,
                        r.Config.DR600Settings.SelfIP,
                        newLon,
                        newLat,
                    )
                    r.SIPConn.WriteToUDP([]byte(lipMsg), term.Addr)
                }
            }
            r.mu.RUnlock()
        }
    }
}