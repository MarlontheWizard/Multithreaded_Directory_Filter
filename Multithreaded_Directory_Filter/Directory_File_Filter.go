/*
@Author: Marlon Dominguez
@Class: Distributed Systems (Rutgers University)
@Date : 03/17/2024

--------------------------------------------------------------------------------
Objective [@Author Michael Palis]:												|
	Given 10 csv files, filter out cities that have a population of atleast		|
	min_pop, which is inputted on the command line. 							|
	Along with min_pop, the command line also takes the directory of the csv 	|
	files as argument. 															|
																				|
	For example:																|
	> go run pop.go us-cities 1000000											|
																				|
	Requirements:																|
	1) Worker Pool																|
	2) Map function																|
	3) Reduce function															|
--------------------------------------------------------------------------------
Approach: 
	Worker Pool	
	-----------
	The worker pool consists of the necessary go routines 
	for the program. 

	A go routine will be created for:
	0) **Default Main**
		Main will crawl through the us-cities directory looking for files. 
		- Create channel of type Map_Task to send work to map function 
		- The work being sent is the file attributes. 
		  Our Map goroutine can then use it to open and filter. 
		- Map_Task will include a channel to store results. 

	1) Map function 
	   The map function will filter a file, giving us records with population > min_pop
	   - There are 3 Map function goroutines generated. 
	   -> If record is valid: Create Map_Task, of type struct, and pass into channel received by Reduce

	2) Reduce function 


*/

package main 


import(

	"fmt"				//Input-Output module
	"encoding/csv"		//CSV file processing module
	"os" 				//Need access to system calls for opening file objects
	"path/filepath"     //Need a method(Walk()) that traverses file system directory 
	"io/fs"				//Needed for the Walk method's call-back function 
	"sync"				//Needed for Wait group
	"strconv"
	
)


/*
Object definition for a job. 
Includes all necessary attributes to complete job. 

@attribute filtered_records: records filtered from file by a worker. 
We want to make filtered_records a buffered channel. This way, the Map
function can continue asynchronously to filter records while Reduce 
completes it's job. 
*/
type Map_Task struct{
	path string
	min_pop int 
	results chan Record
}


/*
Object definition for a record.
A record is considered a line from the csv data. 
A record contains the following format: CityName, StateName, Population.
*/
type Record struct{
	state_name string
	city_name string
	population int
}


/*
Object definition for a state and respective cities 
with population >= min pop.
*/
type State_Obj struct{
	name string
    cities	[]City_Obj
}



/*
Object definition for a city and respective population
with population >= min pop.
*/
type City_Obj struct{
	name string
	population int 
}


func Map(tasks <-chan Map_Task, results chan<- chan Record) {

	//defer wg.Done()
    for job := range tasks {
        file, err := os.Open(job.path)
        if err != nil {
            panic(err)
        }
        defer file.Close()

        file_scanner := csv.NewReader(file)
        file_scanner.FieldsPerRecord = 3

        records, err := file_scanner.ReadAll()
        if err != nil {
            panic(err)
        }

        resultChannel := make(chan Record, 100) // Buffered channel for this task's results
        results <- resultChannel // Send the result channel to the main goroutine

        for _, record := range records {
            rec_population, err := strconv.Atoi(record[2])
            if err != nil {
                panic(err)
            }

            if rec_population >= job.min_pop {
                resultChannel <- Record{state_name: record[1], city_name: record[0], population: rec_population}
            }
        }

	
        close(resultChannel) // Close the result channel after processing all records
    }
}




func Reduce(all_channels chan chan Record, final_data *[]State_Obj, mutex *sync.Mutex) {
    for channel := range all_channels {
        
		for data := range channel {

            // Process data and update final_data
            // Initialize city struct
            city := City_Obj{
                name:       data.city_name,
                population: data.population,
            }

            // Initialize cities slice
            slice := make([]City_Obj, 0)

            // Initialize struct
            struct_obj := State_Obj{
                name:   data.state_name,
                cities: slice,
            }

            // Check if slice is empty, if true then state does not exist
            if len(*final_data) == 0 {
				mutex.Lock()
                //fmt.Printf("State does not exist, creating %s\n", data.state_name)
                slice = append(slice, city)
                *final_data = append(*final_data, struct_obj)
				mutex.Unlock()
                continue

            } else {
                foundit := false

                for x := range *final_data {

                    if (*final_data)[x].name == data.state_name { //State exists, append City_Obj to []cities
						mutex.Lock()
                        //fmt.Printf("State %s exists, not creating\n", data.state_name)
                        (*final_data)[x].cities = append((*final_data)[x].cities, city)
						mutex.Unlock()
                        foundit = true
                        break
                    }
                }

                if foundit == true{
                    continue
                }
                // State doesn't exist
				mutex.Lock()
                //fmt.Printf("State does not exist, creating %s\n", data.state_name)
                slice = append(slice, city)
                *final_data = append(*final_data, struct_obj)
				mutex.Unlock()
            }
          
        }

		break
    }
}

func main() {
    reduce_channels := make(chan chan Record)
    final_data := make([]State_Obj, 0)
    task := make(chan Map_Task, 10)
    results := make(chan chan Record, 100) // Buffered channel for result channels

    var wg sync.WaitGroup

    var mutex sync.Mutex

	//Start Reduce worker threads
	for x := 0; x < 2; x++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            Reduce(reduce_channels, &final_data, &mutex)
        }()
    }

    // Start Map worker threads
    for x := 0; x < 3; x++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            Map(task, results)
        }()
    }

    // Traverse the directory and send tasks to Map worker threads
    go func() {
        defer close(task)

        filepath.Walk(os.Args[1], func(path string, d fs.FileInfo, err error) error {
            if err != nil {
                fmt.Printf("Error!\n")
                return err
            }

            if !d.IsDir() {
				record_pop, err := strconv.Atoi(os.Args[2])

            	if err != nil {
                	panic(err)
            	}

            	task <- Map_Task{path: path, min_pop: record_pop }
            }

            return nil
        })
    }()

    // Read from results channel and forward result channels to reduce_channels
    go func() {
        defer close(results)
        for result := range results {
            reduce_channels <- result // Send the result channel to Reduce worker
        }
    }()


    // Wait for all Map and Reduce tasks to complete
    wg.Wait()

    // Print results
    for _, state := range final_data {
        fmt.Printf("%s: %d\n", state.name, len(state.cities))
        for _, city := range state.cities {
            fmt.Printf("  - %s: %d\n", city.name, city.population)
        }
    }
}


