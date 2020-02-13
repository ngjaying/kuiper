package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-yaml/yaml"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	mode      = "experiment"
	verbal    = false
	current   = 0
	sentCount = 0
	recvCount = 0
	conf      *Conf
)

type Conf struct {
	Kuiper            string `yaml:"kuiper"`
	Emqx              string `yaml:"emqx"`
	SourceTopic       string `yaml:"sourceTopic"`
	SinkTopic         string `yaml:"sinkTopic"`
	StreamSqlTemplate string `yaml:"streamSqlTemplate"`
	RuleName          string `yaml:"ruleName"`
	RuleSqlTemplate   string `yaml:"ruleSqlTemplate"`
	Duration          int    `yaml:"duration"`
	Interval          int    `yaml:"interval"`
	Concurrency       int    `yaml:"concurrency"`
}

type Stream struct {
	Sql string `json:"sql,omitempty"`
}

type Rule struct {
	Id      string                   `json:"id"`
	Sql     string                   `json:"sql"`
	Actions []map[string]interface{} `json:"actions"`
	Options map[string]interface{}   `json:"options"`
}

type message struct {
	Temperature int `json:"temperature"`
	Humidity    int `json:"humidity"`
}

func debugf(format string, a ...interface{}) {
	if verbal {
		fmt.Printf(format, a...)
	}
}

func main() {
	commandPtr := flag.String("command", "create", "The command to run{create|add|clean|send}")
	countPtr := flag.Int("count", 100, "The number of rules to be created")
	modePtr := flag.String("mode", "t", "The mode to run which will affect the prefix of the rule and stream names")
	confPtr := flag.String("conf", "basic", "The conf to be used")
	verbalPtr := flag.Bool("v", false, "print all log or not")
	startPtr := flag.Int("start", -1, "The index to start add or clean")
	flag.Parse()
	mode = *modePtr
	verbal = *verbalPtr
	var err error
	err = loadConf(*confPtr)
	if err != nil {
		panic(err)
	}
	fmt.Printf("start command with configuration %+v\n", conf)
	//get current count
	if *startPtr >= 0 {
		current = *startPtr
	} else {
		err = getCurrent()
		if err != nil {
			panic(err)
		}
		fmt.Printf("currently running %d rules for mode %s\n", current, mode)
	}

	switch *commandPtr {
	case "create":
		fmt.Printf("Create %d rules and send data\n", *countPtr)
		err = createTest(*countPtr)
	case "clean":
		fmt.Printf("Clean %d rules\n", *countPtr)
		clean(*countPtr, *startPtr)
	case "send":
		fmt.Printf("Send data to %d rules\n", *countPtr)
		err = sendData(*countPtr)
	case "add":
		fmt.Printf("Add %d rules\n", *countPtr)
		addRule(*countPtr)
	default:
		panic("unknown command " + *commandPtr)
	}
	if err != nil {
		panic(err)
	}
}

func getCurrent() error {
	resp, err := http.Get(conf.Kuiper + "rules")
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("error get rule count %s", resp.Status)
	}
	defer resp.Body.Close()
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var rules []string
	err = json.Unmarshal(content, &rules)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if strings.HasPrefix(rule, mode+conf.RuleName) {
			current++
		}
	}
	return nil
}

func loadConf(t string) error {
	b, err := ioutil.ReadFile("conf.yaml")
	if err != nil {
		return err
	}
	var cfg map[string]Conf
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return err
	}
	if c, ok := cfg[t]; !ok {
		return fmt.Errorf("No %s config in conf.yaml", t)
	} else {
		conf = &c
	}
	return nil
}

func createTest(count int) error {
	fmt.Printf("start to create %d rules\n", count)
	err := addRule(count)
	if err != nil {
		return err
	}
	fmt.Printf("created %d rules\n", count)
	//send data
	sendData(count)
	return nil
}

func clean(count int, start int) {
	var wg sync.WaitGroup
	tokens := make(chan struct{}, conf.Concurrency)
	last := current - 1
	if start >= 0 {
		last = start + count
	}
	for i := 0; i < count; i++ {
		index := last - i
		if index >= 0 {
			wg.Add(1)
			go func() {
				worker(func(index int) {
					deleteRule(index)
					deleteStream(index)
					printProgress()
				}, tokens, index)
				wg.Done()
			}()
		} else {
			break
		}
	}
	wg.Wait()
	fmt.Println()
}

func addRule(count int) error {
	var wg sync.WaitGroup
	errorCh := make(chan error)
	waitCh := make(chan struct{})
	tokens := make(chan struct{}, conf.Concurrency)
	exeCtx, cancel := context.WithCancel(context.Background())
	for i := 0; i < count; i++ {
		index := current + i
		wg.Add(1)
		go func() {
			worker(func(index int) {
				//create stream
				err := createStream(index)
				if err != nil {
					select {
					case errorCh <- err:
						fmt.Printf("create stream error %d:%v\n", index, err)
						return
					case <-exeCtx.Done():
						fmt.Printf("cancel stream creation %d\n", index)
						return
					}
				}
				//create rule
				err = createRule(index)
				if err != nil {
					select {
					case errorCh <- err:
						fmt.Printf("create rule error %d:%v\n", index, err)
						return
					case <-exeCtx.Done():
						fmt.Printf("cancel rule creation %d\n", index)
						return
					}
				}
				printProgress()
			}, tokens, index)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		fmt.Println()
		close(waitCh)
	}()
	select {
	case err := <-errorCh:
		cancel()
		fmt.Println("receive error, cancel and return: %v", err)
		return err
	case <-waitCh:
		fmt.Println("add rules done")
	}
	return nil
}

func worker(f func(index int), tokens chan struct{}, index int) {
	tokens <- struct{}{}
	defer func() {
		<-tokens
	}()
	f(index)
}

func printProgress() {
	fmt.Print("#")
}

func createStream(index int) error {
	body := &Stream{
		Sql: fmt.Sprintf(conf.StreamSqlTemplate, mode+conf.SourceTopic, index, mode+conf.SourceTopic, index),
	}
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(body)
	resp, err := http.Post(conf.Kuiper+"streams", "application/json", buf)
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 {
		return fmt.Errorf("error creating stream %s", resp.Status)
	}
	return nil
}

func createRule(index int) error {
	//var action = []map[string]interface{}{
	//	{"mqtt": map[string]interface{}{
	//		"server": conf.Emqx,
	//		"topic":  mode + conf.SinkTopic + "/" + strconv.Itoa(index),
	//	}},
	//}
	var action = []map[string]interface{}{
		{"log": map[string]interface{}{}},
	}
	body := &Rule{
		Sql:     fmt.Sprintf(conf.RuleSqlTemplate, mode+conf.SourceTopic, index),
		Id:      mode + conf.RuleName + strconv.Itoa(index),
		Actions: action,
	}
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(body)
	resp, err := http.Post(conf.Kuiper+"rules", "application/json", buf)
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 {
		return fmt.Errorf("error creating stream %d: %v", resp.StatusCode, resp.Body)
	}
	return nil
}

func deleteRule(index int) {
	id := mode + conf.RuleName + strconv.Itoa(index)

	client := &http.Client{}
	req, err := http.NewRequest(http.MethodDelete, conf.Kuiper+"rules/"+id, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	resp, err := client.Do(req)
	if resp.StatusCode != 200 {
		fmt.Printf("error deleting rule %s: %s\n", id, resp.Status)
		return
	}
}

func deleteStream(index int) {
	id := mode + conf.SourceTopic + strconv.Itoa(index)

	client := &http.Client{}
	req, err := http.NewRequest(http.MethodDelete, conf.Kuiper+"streams/"+id, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	resp, err := client.Do(req)
	if resp.StatusCode != 200 {
		fmt.Printf("error deleting stream %s: %s\n", id, resp.Status)
		return
	}
}

func sendData(count int) error {
	client, err := connectMqtt("pub")
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Duration(conf.Interval*1000) * time.Millisecond)
	wait := (conf.Interval * 8e8) / count
	debugf("wait %d nanosecond", wait)
	defer ticker.Stop()
	done := make(chan bool)
	go func() {
		time.Sleep(time.Duration(conf.Duration) * time.Second)
		done <- true
	}()
	//go func() {
	//	client, err := connectMqtt("sub")
	//	if err != nil {
	//		fmt.Println("Cannot connect to mqtt for subscription")
	//	}
	//	client.Subscribe(mode+conf.SinkTopic+"/+", 0, func(client mqtt.Client, msg mqtt.Message) {
	//		debugf("* [%s] %s\n", msg.Topic(), string(msg.Payload()))
	//		recvCount++
	//	})
	//}()
loop:
	for {
		select {
		case <-done:
			fmt.Printf("Totally sent %d, received %d\n", sentCount, recvCount)
			break loop
		case <-ticker.C:
			fmt.Printf("send data intervally %v: %d\n", time.Now(), sentCount)
			for i := 0; i < count; i++ {
				topic := mode + conf.SourceTopic + strconv.Itoa(i)
				message := &message{
					Temperature: rand.Intn(50) - 25,
					Humidity:    rand.Intn(80) + 20,
				}
				payload, err := json.Marshal(message)
				if err != nil {
					return err
				}
				debugf("sending data to topic %s:%s\n", topic, payload)
				client.Publish(topic, 0, false, payload)
				sentCount++
				time.Sleep(time.Duration(wait))
			}
		}
	}
	fmt.Println("Press the Enter Key to terminate the console screen!")
	fmt.Scanln() // wait for Enter Key
	fmt.Printf("Totally sent %d, received %d\n", sentCount, recvCount)
	return nil
}

func connectMqtt(clientId string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().AddBroker(conf.Emqx).SetClientID(clientId)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(3 * time.Second) {
	}
	if err := token.Error(); err != nil {
		return nil, err
	}
	return client, nil
}
