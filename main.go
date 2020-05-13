package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
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

func delete_empty(s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}

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
	pos := 0

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			// This error is a non-EOF error. End the iteration if we encounter
			// an error
			fmt.Println(err)
			break
		}

		codes[pos] = scanner.Text()
		pos++

		if pos >= len(codes) {
			// expand the buffer by 100 again
			newbuf := make([]string, bufferSize)
			codes = append(codes, newbuf...)
		}
	}
	fmt.Println(len(codes))
	codes = delete_empty(codes)
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

// func hitMe(codes pincodes) {
// 	for _, code := range codes {
// 		resp := getPincodeInfo(code)
// 		for _, po := range resp.postOffices {
// 			fmt.Println(po.Block)
// 		}
// 	}
// }

func main() {
	maxGoroutines := flag.Int("maxGoroutines", 2, "the number of goroutines that are allowed to run concurrently")
	flag.Parse()

	codes, err := readFile()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("GOt codes %d \n", len(codes))

	batch := 100
	cityMap := make(map[string]bool)
	cities := []string{}
	concurrency := len(codes) / batch
	if remainder := len(codes) % batch; remainder != 0 {
		concurrency++
	}

	fmt.Printf("Max Goroutines %d \n", *maxGoroutines)
	maxChan := make(chan bool, *maxGoroutines)

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		startIndex := (i * batch) + 1
		endIndex := (startIndex + batch)
		if endIndex > len(codes) {
			endIndex = len(codes)
		}
		maxChan <- true

		fmt.Printf("LOOPING for %d \n", i)

		go func(codes pincodes, maxChan chan bool, idex int) {
			fmt.Printf("GOt codes %d & i of %d\n", len(codes), idex)
			defer wg.Done()
			defer func(maxChan chan bool) { <-maxChan }(maxChan)

			for _, code := range codes {
				resp := getPincodeInfo(code)
				fmt.Printf("Got response for %s \n", code)
				// fmt.Println(resp)
				for _, po := range resp.PostOffice {
					// fmt.Println(po.Block)
					if _, value := cityMap[po.Block]; !value {
						cityMap[po.Block] = true
						cities = append(cities, po.Block)
					}
				}
			}
		}(codes[startIndex:endIndex], maxChan, i)

	}
	wg.Wait()
	fmt.Println("\n\n=============================================")
	fmt.Println(cities)
	fmt.Println("=================================================")
}
