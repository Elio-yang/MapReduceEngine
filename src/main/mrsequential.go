package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"mpe/load"
	"mpe/mr"
	"os"
	"sort"
)

// go run -race mrsequential.go wc.so pg*.txt
func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	// yang:
	// the map and reduce function for special program loaded as plugin
	mapf, reducef := load.LoadPlugin(os.Args[1])
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	// array of {key,value}
	intermediate := []mr.KeyValue{}
	// set of *.txt file
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// store the content
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		// mapf is plugin given eg. in wc.go
		kva := mapf(filename, string(content))
		// get all intermediate (from different txt file)
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
	sort.Sort(mr.ByKey(intermediate))

	// output file generated
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	// for all inter.
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			// how many same key
			// {"hello",1}
			// {"hello",1}
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			// all the times from {key,value}
			// {"1","1","1",...}
			values = append(values, intermediate[k].Value)
		}
		// reduce stage
		// output is a string of times of a special key
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		// xxxxxxxx <key1>|j
		//          	  |i j
		// next key
		i = j
	}

	ofile.Close()
}
