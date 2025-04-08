package main

import (
    "net"
    "time"
)

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