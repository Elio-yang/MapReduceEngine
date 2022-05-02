package main

import (
	"fmt"
	"log"
	"mpe/mr"
	"os"
	"time"
)

// go run -race mrcoordinator.go pg-*.txt
// run this in a windows
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	// m is the coordinator created
	// params is the files to be dealt with
	// pg-being_ernest.txt  pg-grimm.txt pg-sherlock_holmes.txt pg-dorian_gray.txt
	// pg-huckleberry_finn.txt  pg-tom_sawyer.txt pg-frankenstein.txt  pg-metamorphosis.txt
	m := mr.MakeCoordinator(os.Args[1:], 10)

	// weather all job is done
	for m.Done() == false {
		//log.Println("Not DONE!")
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
	log.Println("===================master exit===================")
}
