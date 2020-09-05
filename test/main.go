package main

import (
    "fmt"
    "os"
    "bufio"
    "path"
    "path/filepath"
    "regexp"
    "io"
    "strconv"
    "strings"
    "sort"
    "io/ioutil"
)
type kvInput struct {
	op    uint8 // 0 => get, 1 => put
	key   string
	value string
}

type kvOutput struct {
	value string
}

// Model derived from the Library example
// It has additional functionality , such as append operation,
// which we won't be using for our purposes
var kvModel = Model{
	Partition: func(history []Operation) [][]Operation {
		m := make(map[string][]Operation)
		for _, v := range history {
			key := v.Input.(kvInput).key
			m[key] = append(m[key], v)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ret := make([][]Operation, 0, len(keys))
		for _, k := range keys {
			ret = append(ret, m[k])
		}
		return ret
	},
	PartitionEvent: func(history []Event) [][]Event {
		m := make(map[string][]Event)
		match := make(map[int]string) // id -> key
		for _, v := range history {
			if v.Kind == CallEvent {
				key := v.Value.(kvInput).key
				m[key] = append(m[key], v)
				match[v.Id] = key
			} else {
				key := match[v.Id]
				m[key] = append(m[key], v)
			}
		}
		var ret [][]Event
		for _, v := range m {
			ret = append(ret, v)
		}
		return ret
	},
	Init: func() interface{} {
		// note: we are modeling a single key's value here;
		// we're partitioning by key, so this is okay
		return ""
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(kvInput)
		out := output.(kvOutput)
		st := state.(string)
		if inp.op == 0 {
			// get
			return out.value == st, state
		} else if inp.op == 1 {
			// put
			return true, inp.value
		} else {
			// append
			return true, (st + inp.value)
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(kvInput)
		out := output.(kvOutput)
		switch inp.op {
		case 0:
			return fmt.Sprintf("get('%s') -> '%s'", inp.key, out.value)
		case 1:
			return fmt.Sprintf("put('%s', '%s')", inp.key, inp.value)
		case 2:
			return fmt.Sprintf("append('%s', '%s')", inp.key, inp.value)
		default:
			return "<invalid>"
		}
	},
}

var fileList []string
var rootDir string

// Creates a collection of the file names in the directory
func scanFiles(path string, info os.FileInfo, err error) error {
    if(err != nil){
        return err
    }
    hiddenFile, _ := regexp.Compile(`^[.].*`)
    if info.IsDir() {
        if info.Name() != rootDir {
            fmt.Println("Skipping the Directory found in the path ", path)
            return filepath.SkipDir
        }
    }else{
        if !hiddenFile.MatchString(info.Name()){
            fileList = append(fileList, path)
        }
        
    }
    return nil
}

func readLogFile(filepath string) ([]Operation, error){
    var ops []Operation

    file, err := os.Open(filepath)
    if err != nil {
        return ops, err
    }

    defer file.Close()

    reader := bufio.NewReader(file)

    // LOG FORMAT
    // ClientID, get, Key, Value_read, call_time, return_time
    // ClientID, put, Key, Value_written, call_time, return_time
    getOperation, _ := regexp.Compile(`.*, get,.*,.*,.*,.*`)
    putOperation, _ := regexp.Compile(`.*, put,.*,.*,.*,.*`)

    for{
        lineData, isPrefix, err := reader.ReadLine()
        if err == io.EOF{
            break;
        }else if err!= nil{
            return ops, nil
        }
        if isPrefix {
            fmt.Println("Individual Line Length is larger than ", reader.Size())
            panic("! Cannot handle this case.")
        }

        line := string(lineData)
        args := strings.Split(line, ",")
        for i, _ := range args{
            args[i] = strings.Trim(args[i], " ")
        }

        clientid, _ := strconv.Atoi(args[0])
        callTime, e := strconv.ParseInt(args[4], 10, 64)
        if e != nil {
            return ops, e
        }

        returnTime, e := strconv.ParseInt(args[5], 10, 64)
        if e != nil {
            return ops, e
        }
        
        switch {
        case getOperation.MatchString(line):
            
            ops = append(ops, Operation{clientid, 
                kvInput{op: 0, key: args[2]}, callTime, kvOutput{value: args[3]}, returnTime})


        case putOperation.MatchString(line):
            
            ops = append(ops, Operation{clientid, 
                kvInput{op: 1, key: args[2], value: args[3]}, callTime, kvOutput{}, returnTime})

        }
    }
    return ops, nil
}
func createOperationList(dirPath string) []Operation {
   
    var operationList []Operation
    rootDir = path.Base(dirPath)
    err := filepath.Walk(dirPath, scanFiles)

    if(err != nil){
        fmt.Println("The error is ", err)
        panic("Error walking the Directory")
    }
    
    for _, filename := range fileList {
        fmt.Println("Reading the log file :", filename)
        newEntries, err := readLogFile(filename)
        if(err != nil){
            fmt.Println(err)
            panic("Couldn't read the log entries")  
        }

        operationList = append(operationList, newEntries...)
    }
    fmt.Println("The total number of Operations read is ", len(operationList))
    //fmt.Print(operationList)

    return operationList
}

func visualizeTempFile(model Model, info linearizationInfo) {
	file, err := ioutil.TempFile("./visuals", "*.html")
	if err != nil {
		panic("failed to create temp file")
	}
	err = Visualize(model, info, file)
	if err != nil {
		panic("visualization failed")
	}
	fmt.Println("Created a Visual file ", file.Name())
}


// The program accepts two command line arguments
// Arg 1 -> Specify the relative/absolute path of the logs directory
// Arg 2 -> Binary to indicate whether or not to generate visualization graph
func main(){
    
    if len(os.Args) != 3 {
        fmt.Println("Invoke the binary in the following format : ")
        fmt.Println(" binary <path_of_log_directory> <generate_visualisation>")
        panic("Enter the correct number of arguments")
    }
    list := createOperationList(os.Args[1])
    result := CheckOperations(kvModel, list)
    if result == true{
        fmt.Println("\nThe history is linearizable!!")
    }else{
        fmt.Println("The history is not linearizable.")
    }

    // Make sure you manually delete the temp files generated by each visualization
    if os.Args[2] == "1" {
        _ , info := CheckOperationsVerbose(kvModel, list, 0)
        computeVisualizationData(kvModel, info)
        visualizeTempFile(kvModel, info)
    }
    
}