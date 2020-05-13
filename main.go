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

	"github.com/gomodule/redigo/redis"
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

func populatePincodesInRedis(pool *redis.Pool) (err error) {
	var file *os.File

	file, err = os.Open("pincodes.txt")
	if err != nil {
		fmt.Println(err)
		fmt.Println("Inside populatePincodesInRedis")
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	conn := pool.Get()
	defer conn.Close()

	r, _ := regexp.Compile("[0-9]+")

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			// This error is a non-EOF error. End the iteration if we encounter
			// an error
			fmt.Println(err)
			break
		}

		x := scanner.Text()
		if r.MatchString(x) {
			err := db.AppendToPincodes(conn, x)
			if err != nil {
				fmt.Println("Inside populatePincodesInRedis scanner block")
				return err
			}
		}
	}
	return
}

func getPincodeInfo(word string) (pincodeResp pincodeAPIResponse) {
	apiEndPoint := fmt.Sprintf("https://api.postalpincode.in/pincode/%s", word)
	fmt.Printf("Hitting api for %s \n", apiEndPoint)

	resp, err := http.Get(apiEndPoint)
	if err != nil {
		fmt.Println(err)
		fmt.Println("Inside getPincodeInfo http")
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Got Problem in reading for pincode %s", word)
		log.Fatal("error %v", err)
	}

	fmt.Printf("Read all for %s \n", word)

	var r apiResponse
	err = json.Unmarshal(body, &r)
	if err != nil {
		log.Fatalf("Inside getPincodeInfo unmarshal %v at line 105", err)
	}

	fmt.Printf("Unmarshalled for %s \n", word)

	pincodeResp = r[0]
	return
}

func performWork(pool *redis.Pool, maxGoroutines int, workLength int) (cities pincodes, err error) {
	var wg sync.WaitGroup

	concurrency := pincodeutils.GetConcurrency(workLength)
	maxChan := make(chan bool, maxGoroutines)

	wg.Add(concurrency)
	fmt.Printf("My Concurrency %d\n", concurrency)

	for i := 0; i < concurrency; i++ {
		conn := pool.Get()
		defer conn.Close()
		codes, err := db.PopList(conn, pincodeutils.PincodeListKey, pincodeutils.BatchSize)
		if err != nil {
			log.Fatalf("Inside FOr loop concurrency with error %v at line 128", err)
		}

		maxChan <- true

		fmt.Printf("LOOPING for %d \n", i)
		go func(codes pincodes, maxChan chan bool, pool *redis.Pool) {
			newConn := pool.Get()

			defer wg.Done()
			defer newConn.Close()
			defer func(maxChan chan bool) { <-maxChan }(maxChan)

			for _, code := range codes {
				resp := getPincodeInfo(code)
				log.Printf("Got response for code %s\n", code)
				for _, po := range resp.PostOffice {
					err = db.AppendToCities(conn, po.Block)
					if err != nil {
						log.Fatalf("Inside go-routine postoffice loop %v", err)
					}
				}
			}
		}(codes, maxChan, pool)
	}
	wg.Wait()

	conn := pool.Get()
	defer conn.Close()
	cities, err = db.ListCities(conn)
	if err != nil {
		fmt.Println("Inside performWork list cities")
		log.Fatal("error %v", err)
		return
	}
	return

}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	maxGoroutines := flag.Int("maxGoroutines", 2, "the number of goroutines that are allowed to run concurrently")
	flag.Parse()
	fmt.Printf("Max Goroutines %d \n", *maxGoroutines)
	pool := db.NewPool()
	conn := pool.Get()
	_ = db.ResetList(conn, pincodeutils.PincodeListKey)

	err := populatePincodesInRedis(pool)
	if err != nil {
		fmt.Println("Inside Main populate Pincodes err")
		fmt.Println(err)
	}
	workLength, err := db.LengthOfList(conn, pincodeutils.PincodeListKey)
	if err != nil {
		fmt.Println("Inside Main populate Length of the list err")
		log.Fatal("error %v", err)
		return
	}

	fmt.Printf("GOt codes %d \n", workLength)

	cities, _ := performWork(pool, *maxGoroutines, workLength)

	fmt.Println("\n\n=============================================")
	fmt.Println(cities)
	fmt.Println("=================================================")
}
