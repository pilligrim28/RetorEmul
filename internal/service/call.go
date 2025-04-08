package main

import (
	"fmt"
	"log"
	"math"
	"net"
	 "retroemul/internal/models"
    "retroemul/config"
	"time"
)

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

func (r *Repeater) startAudioStream(call *Call) {
	const (
		packetSize   = 160 // 20ms at 8000 Hz
		packetPeriod = 20 * time.Millisecond
	)

	callInfoInterval := time.Duration(r.Config.DR600Settings.CallInfoInterval) * time.Second
	if callInfoInterval <= 0 {
		callInfoInterval = 5 * time.Second
	}

	r.mu.RLock()
	audioData, exists := r.AudioFiles[call.SourceID%3+1]
	r.mu.RUnlock()

	if !exists {
		audioData = generateDefaultAudio()
	}

	ticker := time.NewTicker(packetPeriod)
	defer ticker.Stop()

	infoTicker := time.NewTicker(callInfoInterval)
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
	for id := range r.Terminals {
		if id != sourceID {
			call.Participants[id] = true
		}
	}
	r.ActiveCalls[callID] = call
	r.mu.Unlock()

	log.Printf("Initiated test call %s from %d to group %d", callID, sourceID, groupID)
	r.sendCallUpdate(call)
	go r.startAudioStream(call)
}

func generateDefaultAudio() []byte {
	const (
		sampleRate = 8000
		duration   = 2 * time.Second
		freq       = 440
	)

	samples := int(sampleRate * duration.Seconds())
	data := make([]byte, samples)

	for i := 0; i < samples; i++ {
		val := math.Sin(2 * math.Pi * freq * float64(i) / float64(sampleRate))
		data[i] = byte(127 * (1 + val))
	}

	return data
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

