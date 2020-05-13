package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go-pincode-scanner/db"
	"go-pincode-scanner/pincodeutils"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sync"
)

type apiResponse []pincodeAPIResponse

type pincodeAPIResponse struct {
	Message    string
	Status     string
	PostOffice []postOffice
}

type postOffice struct {
	Name           string
	Description    string
	BranchType     string
	DeliveryStatus string
	Circle         string
	District       string
	Division       string
	Region         string
	Block          string
	State          string
	Country        string
	Pincode        string
}

type pincodes []string

func readFile() (codes pincodes, err error) {
	var file *os.File

	file, err = os.Open("pincodes.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	// initial size of our wordlist
	bufferSize := 100
	codes = make(pincodes, bufferSize)
	r, _ := regexp.Compile("[0-9]+")
	pos := 0

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			// This error is a non-EOF error. End the iteration if we encounter
			// an error
			fmt.Println(err)
			break
		}

		x := scanner.Text()
		if r.MatchString(x) {
			codes[pos] = x
			pos++
		}

		if pos >= len(codes) {
			// expand the buffer by 100 again
			newbuf := make([]string, bufferSize)
			codes = append(codes, newbuf...)
		}
	}
	fmt.Println(len(codes))
	codes = pincodeutils.DeleteEmpty(codes)
	return
}

func getPincodeInfo(word string) (pincodeResp pincodeAPIResponse) {
	apiEndPoint := fmt.Sprintf("https://api.postalpincode.in/pincode/%s", word)
	fmt.Printf("Hitting api for %s \n", apiEndPoint)

	resp, err := http.Get(apiEndPoint)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	fmt.Printf("Read all for %s \n", word)

	var r apiResponse
	err = json.Unmarshal(body, &r)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Unmarshalled for %s \n", word)

	pincodeResp = r[0]
	return
}

func performWork(codes pincodes, maxGoroutines int) (cities pincodes, err error) {
	concurrency := pincodeutils.GetConcurrency(len(codes))
	maxChan := make(chan bool, maxGoroutines)
	workLength := len(codes)
	pool := db.NewPool()
	conn := pool.Get()
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		startIndex, endIndex := pincodeutils.GetPartitionIndexes(workLength, i)
		maxChan <- true

		fmt.Printf("LOOPING for %d \n", i)

		go func(codes pincodes, maxChan chan bool, idex int) {
			fmt.Printf("GOt codes %d & i of %d\n", workLength, idex)

			defer wg.Done()
			defer func(maxChan chan bool) { <-maxChan }(maxChan)

			for _, code := range codes {
				resp := getPincodeInfo(code)
				// fmt.Printf("Got response for %s \n", code)

				for _, po := range resp.PostOffice {
					err = db.AppendToCities(conn, po.Block)
					if err != nil {
						log.Fatal(err)
					}
				}
			}
		}(codes[startIndex:endIndex], maxChan, i)

	}
	wg.Wait()

	cities, err = db.ListCitites(conn)
	if err != nil {
		log.Fatal(err)
		return
	}
	return

}

func main() {
	maxGoroutines := flag.Int("maxGoroutines", 2, "the number of goroutines that are allowed to run concurrently")
	flag.Parse()
	fmt.Printf("Max Goroutines %d \n", *maxGoroutines)

	pinCodes, err := readFile()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("GOt codes %d \n", len(pinCodes))

	cities, _ := performWork(pinCodes, *maxGoroutines)

	fmt.Println("\n\n=============================================")
	fmt.Println(cities)
	fmt.Println("=================================================")
}
