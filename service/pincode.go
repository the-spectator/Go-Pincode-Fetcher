package service

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	gbp "github.com/vivekkundariya/go-batch-processor/rate_limit"
)

type PinCodeResponses struct {
	PinCodeResponse []PinCodeResponse
}

type PinCodeResponse struct {
	Message    string       `json:"Message"`
	Status     string       `json:"Status"`
	PostOffice []PostOffice `json:"PostOffice"`
}
type PostOffice struct {
	Name           string      `json:"Name"`
	Description    interface{} `json:"Description"`
	BranchType     string      `json:"BranchType"`
	DeliveryStatus string      `json:"DeliveryStatus"`
	Circle         string      `json:"Circle"`
	District       string      `json:"District"`
	Division       string      `json:"Division"`
	Region         string      `json:"Region"`
	Block          string      `json:"Block"`
	State          string      `json:"State"`
	Country        string      `json:"Country"`
	Pincode        string      `json:"Pincode"`
}

type PinCodeInput struct {
	PinCode string
}

type PinCodeWorker struct {
	OutputChan chan *PinCodeResponses
	Inputs     []*PinCodeInput
	FileName   string
}

func (unw *PinCodeWorker) WorkSize() int {
	return len(unw.Inputs)
}

func (unw *PinCodeWorker) GetInput(index int) (interface{}, error) {

	return unw.Inputs[index], nil
}
func (unw *PinCodeWorker) Work(object interface{}) (interface{}, error) {
	var input *PinCodeInput = object.(*PinCodeInput)
	url := fmt.Sprintf("https://api.postalpincode.in/pincode/%s", input.PinCode)
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
	}

	res, err := client.Do(req)
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)

	var response []PinCodeResponse
	if err = json.Unmarshal(b, &response); err != nil {
		print(err)
		return nil, err
	}
	return &PinCodeResponses{PinCodeResponse: response}, nil
}

func (unw *PinCodeWorker) ProcessResp(resp interface{}, in interface{}) {
	unw.OutputChan <- resp.(*PinCodeResponses)
}

func (unw *PinCodeWorker) HandleOutput() {
	var outputs []*PinCodeResponses
	for out := range unw.OutputChan {
		outputs = append(outputs, out)
	}
}

func (unw *PinCodeWorker) Close() {
	close(unw.OutputChan)
}

func parseInput(filename string) []*PinCodeInput {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var pincodeInputs []*PinCodeInput

	for scanner.Scan() {
		pincodeInputs = append(pincodeInputs, &PinCodeInput{PinCode: scanner.Text()})
	}
	return pincodeInputs
}

func DoPincodeFetchJob(filename string, rate int) {
	inputs := parseInput(filename)
	outputChan := make(chan *PinCodeResponses)
	rateEx := gbp.RateLimitExecutor{Limit: rate}
	pinCodeWorker := &PinCodeWorker{OutputChan: outputChan, Inputs: inputs, FileName: filename}
	rateEx.Execute(pinCodeWorker)
}
