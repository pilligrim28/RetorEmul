package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

"retroemul/config"
"retroemul/internal/server"

)
type Repeater struct {
    Config         *config.Config
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

func NewRepeater(cfg *config.Config) *Repeater{
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
func (r *Repeater) loadAudioFiles() error {
    for fileNum, filePath := range r.Config.DR600Settings.AudioFiles {
        data, err := os.ReadFile(filePath)
        if err != nil {
            return fmt.Errorf("failed to load audio file %s: %w", filePath, err)
        }
        r.AudioFiles[fileNum] = data
    }
    return nil
}

func (r *Repeater) sendToDispatcher(data string) {
    if r.DispatcherAddr == nil {
        return
    }
    r.SIPConn.WriteToUDP([]byte(data), r.DispatcherAddr)
}

// Добавьте в repeater.go

func (r *Repeater) runCallSimulator() {
    defer r.wg.Done()
    if !r.Config.DR600Settings.CallsEnable {
        return
    }

    for _, testCase := range r.Config.DR600Settings.TestCases {
        r.wg.Add(1)
        go func(tc TestCase) {
            defer r.wg.Done()
            delay := time.Duration(r.Config.DR600Settings.MinDelay + 
                rand.Intn(r.Config.DR600Settings.MaxDelay - r.Config.DR600Settings.MinDelay)) * 
                time.Second
            time.Sleep(delay)
            r.initiateTestCall(tc.SourceID, tc.GroupID, tc.Slot, tc.FileNum)
        }(testCase)
    }
}

func (r *Repeater) checkNetwork() {
    if r.DispatcherAddr == nil {
        return
    }
    conn, err := net.Dial("udp", r.DispatcherAddr.String())
    if err != nil {
        log.Printf("Ошибка подключения к диспетчеру: %v", err)
    } else {
        conn.Close()
        log.Println("Соединение с диспетчером установлено")
    }
}