package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	method    = "POST"
	url       = "http://192.168.0.110:6041/rest/sql"
	username  = "root"
	password  = "taosdata"
	database  = "benchmark_db"
	stable    = "window_state_room"
	batchSize = 5000
	thread    = 4
	reader    *bufio.Reader
	once      sync.Once
	mutex 	  = &sync.Mutex{}
	count	  = 0
	isEOF	  = false
	sum int64 = 0
	wg        sync.WaitGroup
	currentThread = 0
)




func main() {
	flag.IntVar(&thread,"thread",thread,"specify the number of threads to start")
	flag.IntVar(&batchSize, "batchsize",batchSize,"specify the number of points loaded in each batch")
	flag.Parse()
	runtime.GOMAXPROCS(thread)
	Run()
	time.Sleep(time.Second * 5)
	wg.Wait()
	fmt.Println(sum)
}



func Run() {
	for  {
		if currentThread < thread {
			go HttpWrite(strings.NewReader(ReadData()))
		}
		if isEOF {
			return
		}
	}
}

type Insert struct {
	table string
	tag string
	value string

}

type Inserts []Insert

func ReadData() string {
	buf := NewReader()
	var body string
	var inserts Inserts

	for  {
		if len(inserts) == batchSize {
			body = ConvToString(&inserts)
			return body
		}
		_ = make([]byte, 4<<20) // 4MB
		b, errR := buf.ReadBytes('\n')
		if errR != nil {
			if errR == io.EOF {
				isEOF = true
				break
			}
			fmt.Println(errR.Error())
		}
		strs := strings.Split(string(b),"exted")
		if len(strs) != 3 {
			continue
		}
		insert := Insert{
			strs[0],
			strs[1],
			strs[2],
		}
		inserts = append(inserts,insert)
	}


	return ConvToString(&inserts)
}

func ConvToString(inserts *Inserts) string{
	result := strings.Builder{}
	result.WriteString("INSERT INTO ")
	var body  = make(map[string]string)
	for _,insert := range *inserts {
		if len(body[insert.table]) > 0 {
			body[insert.table] = body[insert.table] + " " + strings.Replace(insert.value,"\n"," ",-1)
		} else {
			body[insert.table] = database +"."+ insert.table + " USING " +database +"." + stable + " TAGS"+insert.tag + " VALUES"+strings.Replace(insert.value,"\n"," ",-1)
		}
	}
	for _,str := range body {
		result.WriteString("\n")
		result.WriteString(str)
	}
	inserts = nil
	return result.String()
}






func NewReader() *bufio.Reader {
	once.Do(func() {
		reader = bufio.NewReader(os.Stdin)
	})
	return reader
}


// INSERT INTO t_1_0000000000000_0000000000001_1 USING window_state_room TAGS(1,0000000000000,0000000000001,1) VALUES ('2018-01-01T00:00:00Z',0.0000000000000000,3.2000000000000002);

func HttpWrite(data io.Reader) {
	count ++
	wg.Add(1)
	start := time.Now().UnixNano()
	request, err := http.NewRequest(method, url, data)
	if err != nil {
		log.Println(err)
	}
	request.SetBasicAuth(username, password)
	client := http.Client{}
	response, err := client.Do(request)
	defer response.Body.Close()
	if err != nil {
		log.Println(err)
	}
	if response.StatusCode != 200 {
		log.Printf("field: %d\n", response.StatusCode)
	}
	end := time.Now().UnixNano()
	sum = sum + (end - start)
	count --
	wg.Done()

}
