package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go-pincode-scanner/db"
	"go-pincode-scanner/service"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"sync"
	"time"

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
		log.Fatalf("Error opening the file with err %v", err)
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
			log.Fatalf("Errors in scanning %v", err)
			break
		}

		x := scanner.Text()
		if r.MatchString(x) {
			err := db.AppendToPincodes(conn, x)
			if err != nil {
				log.Fatalf("Inside populatePincodesInRedis scanner block with err %v", err)
				return err
			}
		}
	}
	return
}

func getPincodeInfo(word string, conn redis.Conn) (pincodeResp pincodeAPIResponse) {
	apiEndPoint := fmt.Sprintf("https://api.postalpincode.in/pincode/%s", word)
	// log.Printf("Hitting api for %s \n", apiEndPoint)

	resp, err := http.Get(apiEndPoint)
	if err != nil {
		log.Fatalf("Problem in http Get with error %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Got Problem in reading for pincode %s with error %v", word, err)
	}
	db.IncrementAPICounter(conn)

	// log.Printf("Read all for %s \n", word)

	var r apiResponse
	err = json.Unmarshal(body, &r)
	if err != nil {
		log.Fatalf("Inside getPincodeInfo unmarshal %v at line 105", err)
	}

	// log.Printf("Unmarshalled for %s \n", word)

	pincodeResp = r[0]
	return
}

func performWork(pool *redis.Pool, workLength int) (err error) {
	var wg sync.WaitGroup

	concurrency := service.GetConcurrency(workLength)
	maxChan := make(chan bool, service.MaxGoroutines)

	// wg.Add(concurrency)
	log.Printf("My Concurrency %d\n", concurrency)

	for i := 0; i < concurrency; i++ {
		conn := pool.Get()
		defer conn.Close()
		codes, err := db.PopList(conn, service.PincodeListKey, service.BatchSize)
		if err != nil {
			log.Fatalf("Inside FOr loop concurrency with error %v at line 128", err)
		}

		maxChan <- true
		wg.Add(1)

		log.Printf("LOOPING for %d \n", i)
		go func(codes pincodes, maxChan chan bool, pool *redis.Pool, index int) {
			newConn := pool.Get()

			defer wg.Done()
			defer newConn.Close()
			defer func() { log.Printf("Work Done for job %d", index) }()
			defer func(maxChan chan bool) { <-maxChan }(maxChan)

			for _, code := range codes {
				resp := getPincodeInfo(code, newConn)
				// log.Printf("Got response for code %s\n", code)
				for _, po := range resp.PostOffice {
					err = db.AppendToCities(conn, po.Block)
					if err != nil {
						log.Fatalf("Inside go-routine postoffice loop %v", err)
					}
				}
			}
		}(codes, maxChan, pool, i)
	}
	wg.Wait()
	return

}

func parseFlags() {
	maxGoroutines := flag.Int("maxGoroutines", 2, "the number of goroutines that are allowed to run concurrently")
	batchSize := flag.Int("batchSize", 500, "the batch size of pincodes to be processed by go routine")

	flag.Parse()
	service.MaxGoroutines = *maxGoroutines
	service.BatchSize = *batchSize

	log.Printf("Max Goroutines %d \n", service.MaxGoroutines)
	log.Printf("Batch Size %d \n", service.BatchSize)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	runtime.GOMAXPROCS(runtime.NumCPU())

	parseFlags()
	pool := db.NewPool()
	conn := pool.Get()

	_ = db.ResetList(conn, service.PincodeListKey)
	db.ResetAPICounter(conn)

	err := populatePincodesInRedis(pool)
	if err != nil {
		log.Fatalf("Inside Main populate Pincodes err %v", err)
	}

	startTime := time.Now()
	log.Printf("Starting the at %v", startTime)

	workLength, err := db.LengthOfList(conn, service.PincodeListKey)
	if err != nil {
		log.Fatalf("Inside Main populate Length of the list err %v", err)
		return
	}

	log.Printf("GOt codes %d \n", workLength)

	_ = performWork(pool, workLength)

	defer func() {
		log.Printf("Number of request hit are %d", db.GetAPICounter(conn))
		log.Printf("Completed the Process in %v", time.Since(startTime))
	}()

	log.Println("\n\n=============================================")
	cities, _ := db.ListCities(conn)
	log.Println(cities)
	log.Println("=================================================")
}
