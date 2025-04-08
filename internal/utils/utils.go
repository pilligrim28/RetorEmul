package main

import (
    "fmt"
    "math/rand"
    "strings"
)

func parseSIPHeaders(msg string) map[string]string {
    headers := make(map[string]string)
    for _, line := range strings.Split(msg, "\r\n") {
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