package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
)

// format : <string: station name>;<double: measurement>

// Get min, mean, and max

//type Line struct {
//	city         string
//	temperatures []float32
//}

type TemperatureStats struct {
	min   float64
	max   float64
	total float64
	count float64
}

type Line struct {
	city        string
	temperature string
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

// TODO channel orientation ?
func readFile(c chan Line, chanReadCompleted chan bool) {
	file, err := os.Open(flag.Arg(len(flag.Args()) - 1))
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	count := 0

	for scanner.Scan() {
		values := strings.Split(scanner.Text(), ";")
		count++

		// if count == 10000000 {
		// 	break
		// }

		c <- Line{city: values[0], temperature: values[1]}
	}

	chanReadCompleted <- true
	close(c)
	close(chanReadCompleted)
}

type Toto struct {
	chanLine chan Line
	chanStat chan TemperatureStats
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Ceil(val*ratio) / ratio
}

func main() {
	flag.Parse()

	log.Println(flag.Args()[len(flag.Args())-1:])

	log.Println(*cpuprofile)

	if *cpuprofile != "" {
		log.Println("add profiling")
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	chanLine := make(chan Line, 100000)
	chanReadCompleted := make(chan bool)

	go readFile(chanLine, chanReadCompleted)

	channels := make(map[string]Toto)

	cityCompute := func(toto Toto) {
		result := TemperatureStats{}
		for b := range toto.chanLine {
			// log.Printf("city: %s, temperature: %s", b.city, b.temperature)
			temp, err := strconv.ParseFloat(b.temperature, 64)
			if err != nil {
				log.Printf("error parsing temperature: %s", b.temperature)
				continue
			}

			if result.min == 0 || temp < result.min {
				result.min = temp
			}
			if result.max == 0 || temp > result.max {
				result.max = temp
			}

			result.total += temp
			result.count++
			// log.Printf("count: %f", result.count)
		}

		toto.chanStat <- result
		close(toto.chanStat)
	}

	// Route into city channels
	go func() {
		for b := range chanLine {
			toto, ok := channels[b.city]
			if !ok {
				// log.Printf("new city channel created: %s", b.city)
				cityChannel := make(chan Line, 10000)
				// TODO buffer size
				resChannel := make(chan TemperatureStats)
				toto = Toto{chanLine: cityChannel, chanStat: resChannel}
				channels[b.city] = toto
				go cityCompute(channels[b.city])
			}
			// log.Printf("route into city channel: %s", b.city)
			toto.chanLine <- b
			// TODO Gracefully stop
		}
		for _, v := range channels {
			close(v.chanLine)
			// log.Printf("close channel: %s", k)
		}

		// log.Println(" end Route into city channels")
	}()

	// Wait for read to complete

	// log.Println("waiting read completed")
	<-chanReadCompleted
	// log.Println("read completed from main")
	// time.Sleep(time.Second)
	// log.Println(channels)

	keys := make([]string, 0, len(channels))
	for k := range channels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Printf("{")
	for i, k := range keys {
		if i != 0 {
			fmt.Print(", ")
		}
		a := <-channels[k].chanStat
		// log.Printf("city: %s, min: %f, total: %f, count: %f, avg: %f, max: %f", k, a.min, a.total, a.count, a.total/a.count, a.max)
		fmt.Printf("%s=%.1f/%.1f/%.1f",
			k,
			roundFloat(a.min, 1),
			roundFloat(a.total/a.count, 1),
			roundFloat(a.max, 1),
		)
	}

	fmt.Printf("}\n")
}
