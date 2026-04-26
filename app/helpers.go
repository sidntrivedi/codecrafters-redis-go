package main

import "time"

func isExpired(val ValueEntry) bool {
	return val.hasExpiry && time.Now().After(val.expiresAt)
}
