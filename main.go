package main

import (
	"flag"

	"github.com/the-spectator/go-pincode-scanner/service"
)

func main() {
	fileName := flag.String("file", "", "File name")
	rate := flag.Int("rl", 50, "Rate Limit")
	flag.Parse()
	service.DoPincodeFetchJob(*fileName, *rate)
}
