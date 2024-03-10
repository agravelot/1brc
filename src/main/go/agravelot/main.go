package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TemperatureStats struct {
	city  string
	min   float64
	max   float64
	total float64
	count float64
}

type Line struct {
	city        string
	temperature string
}

type Toto struct {
	chanLine chan Line
	chanStat chan TemperatureStats
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

	for scanner.Scan() {
		values := strings.Split(scanner.Text(), ";")
		c <- Line{city: values[0], temperature: values[1]}
	}

	close(c)
	close(chanReadCompleted)
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Ceil(val*ratio) / ratio
}

// TODO Add chan orientation
func routeMessage(channels map[byte]Toto, chanLine chan Line) {
	for b := range chanLine {
		toto, ok := channels[b.city[0]]
		if !ok {
			cityChannel := make(chan Line, 100000)
			resChannel := make(chan TemperatureStats)
			toto = Toto{chanLine: cityChannel, chanStat: resChannel}
			channels[b.city[0]] = toto
			go citiesCompute(channels[b.city[0]])
		}
		toto.chanLine <- b
	}

	for _, v := range channels {
		close(v.chanLine)
	}
}

func citiesCompute(toto Toto) {
	results := make(map[string]TemperatureStats)

	for b := range toto.chanLine {
		temp, err := strconv.ParseFloat(b.temperature, 64)
		if err != nil {
			panic(err)
		}

		result, ok := results[b.city]
		if !ok {
			results[b.city] = TemperatureStats{min: temp, max: temp, total: temp, count: 1, city: b.city}
			continue
		}

		if temp < result.min {
			result.min = temp
		} else if temp > result.max {
			result.max = temp
		}

		result.total += temp
		result.count++

		results[b.city] = result
	}

	// Order by city name
	keys := make([]string, 0, len(results))
	for k := range results {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	for _, k := range keys {
		toto.chanStat <- results[k]
	}

	close(toto.chanStat)
}

func main() {
	flag.Parse()

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

	chanLine := make(chan Line, 1000000)
	chanReadCompleted := make(chan bool)

	go readFile(chanLine, chanReadCompleted)

	channels := make(map[byte]Toto)

	// Route into city channels
	go routeMessage(channels, chanLine)
	time.Sleep(1 * time.Second)

	<-chanReadCompleted
	keys := make([]byte, 0, len(channels))
	for k := range channels {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	count := 0

	fmt.Print("{")
	for _, k := range keys {
		// if i != 0 {
		// 	fmt.Print(", ")
		// }
		toto, ok := channels[k]
		if !ok {
			panic("channel not found")
		}

		for v := range toto.chanStat {
			if count != 0 {
				fmt.Print(", ")
			}

			fmt.Printf("%s=%.1f/%.1f/%.1f",
				v.city,
				roundFloat(v.min, 1),
				roundFloat(v.total/v.count, 1),
				roundFloat(v.max, 1),
			)
			count++
		}
	}

	fmt.Print("}\n")
}
