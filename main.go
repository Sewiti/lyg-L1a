package main

import (
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
)

func main() {
	for i := 1; i <= 3; i++ {
		fmt.Printf("Started %d\n", i)
		dataFilename := fmt.Sprintf("IFF-8-5_BernotasM_L1_dat_%d.json", i)
		resultsFilename := fmt.Sprintf("IFF-8-5_BernotasM_L1_rez_%d.txt", i)

		items := readJSON(dataFilename)

		dataMonitor := newDataMonitor(5)
		resultMonitor := newResultMonitor()

		waiter := sync.WaitGroup{}
		waiter.Add(len(items))

		for j := 0; j < 6; j++ {
			go worker(&dataMonitor, &resultMonitor, &waiter)
		}

		putIntoMonitor(&dataMonitor, items)

		waiter.Wait()

		outputResults(resultsFilename, items, resultMonitor.getItems())
		fmt.Printf("Finished %d\n", i)
	}
}

type data struct {
	Name     string  `json:"name"`
	Age      int     `json:"age"`
	Salary   float64 `json:"salary"`
	Computed string
}

// Functions
func readJSON(filename string) []data {
	file, err := os.Open(filename)

	if err != nil {
		panic(err)
	}
	defer file.Close()

	byteValue, _ := ioutil.ReadAll(file)

	var data []data
	json.Unmarshal(byteValue, &data)

	return data
}

func putIntoMonitor(dataMonitor *dataMonitor, items []data) {
	for _, v := range items {
		dataMonitor.addItem(v)
	}
}

func worker(dataMonitor *dataMonitor, resultMonitor *resultMonitor, waiter *sync.WaitGroup) {
	for {
		item := dataMonitor.removeItem()

		execute(resultMonitor, item)

		waiter.Done()
	}
}

func execute(resultMonitor *resultMonitor, item data) {
	if item.Age < 18 {
		return
	}

	bytes := []byte(fmt.Sprintf("%s:%d:%f", item.Name, item.Age, item.Salary))

	hasher := sha512.New()
	for i := 0; i < 9e6; i++ {
		hasher.Write(bytes)
	}

	item.Computed = base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	resultMonitor.addItemSorted(item)
}

func outputResults(filename string, initData []data, results []data) {
	file, err := os.Create(filename)

	if err != nil {
		panic(err)
	}
	defer file.Close()

	file.WriteString(fmt.Sprintf("Initial data:\n%-30s|%4s | %-9s\n", "Name", "Age", "Salary"))
	file.WriteString("------------------------------+-----+----------\n")

	if len(initData) > 0 {
		for _, v := range initData {
			file.WriteString(fmt.Sprintf("%-30s|%4d |%9.2f\n", v.Name, v.Age, v.Salary))
		}
	} else {
		file.WriteString(fmt.Sprintf("%-30s|%4s |%9s\n", "-", "-", "--.--"))
	}

	file.WriteString(fmt.Sprintf("\nResults:\n%-30s|%4s | %-9s| %s\n", "Name", "Age", "Salary", "Hash"))
	file.WriteString("------------------------------+-----+----------+------------------------------------------------------------------------------------------\n")

	if len(results) > 0 {
		for _, v := range results {
			file.WriteString(fmt.Sprintf("%-30s|%4d |%9.2f | %s\n", v.Name, v.Age, v.Salary, v.Computed))
		}
	} else {
		file.WriteString(fmt.Sprintf("%-30s|%4s |%9s | %s\n", "-", "-", "--.--", "-"))
	}
}

// Data Monitor
type dataMonitor struct {
	container []data
	size      int
	from      int
	to        int
	mutex     *sync.Mutex
	cond      *sync.Cond
}

func newDataMonitor(size int) dataMonitor {
	mutex := sync.Mutex{}

	return dataMonitor{
		container: make([]data, size),
		mutex:     &mutex,
		cond:      sync.NewCond(&mutex),
	}
}

func (monitor *dataMonitor) addItem(item data) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	for monitor.size == len(monitor.container) {
		monitor.cond.Wait()
	}

	monitor.container[monitor.to] = item
	monitor.to = (monitor.to + 1) % len(monitor.container)
	monitor.size++

	monitor.cond.Broadcast()
}

func (monitor *dataMonitor) removeItem() data {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	for monitor.size <= 0 {
		monitor.cond.Wait()
	}

	var item = monitor.container[monitor.from]
	monitor.container[monitor.from] = data{} // nebutinas, bet padeda debuggint
	monitor.from = (monitor.from + 1) % len(monitor.container)
	monitor.size--

	monitor.cond.Broadcast()

	return item
}

// Result Monitor
type resultMonitor struct {
	container []data
	mutex     *sync.Mutex
}

func newResultMonitor() resultMonitor {
	return resultMonitor{
		mutex: &sync.Mutex{},
	}
}

func (monitor *resultMonitor) addItemSorted(item data) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	var i int
	for i = 0; i < len(monitor.container); i++ {
		if monitor.container[i].Name >= item.Name {
			break
		}
	}

	monitor.container = append(monitor.container, data{})

	copy(monitor.container[i+1:], monitor.container[i:])

	monitor.container[i] = item
}

func (monitor *resultMonitor) getItems() []data {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	return monitor.container
}
