package main

import (
	"context"
	"engine/common"
	"engine/xsql/processors"
	"engine/xstream"
	"engine/xstream/sinks"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"path"
	"strings"
	"time"
)
var dataDir string
var log = common.Log

type RuleState struct{
	Name string
	Topology *xstream.TopologyNew
	Triggered bool
}
type RuleRegistry map[string]*RuleState
var registry RuleRegistry
var processor *processors.RuleProcessor

type Server int

var QUERY_RULE_ID = "internal-xstream_query_rule"
func (t *Server) CreateQuery(sql string, reply *string) error {
	if _, ok := registry[QUERY_RULE_ID]; ok {
		stopQuery()
	}
	tp, err := processors.NewRuleProcessor(path.Dir(dataDir)).ExecQuery(QUERY_RULE_ID, sql)
	if err != nil {
		msg := fmt.Sprintf("failed to create query: %s.", err)
		log.Println(msg)
		return fmt.Errorf(msg)
	} else {
		rs := &RuleState{Name: QUERY_RULE_ID, Topology: tp, Triggered: true}
		registry[QUERY_RULE_ID] = rs
		msg := fmt.Sprintf("query is submit successfully.")
		log.Println(msg)
		*reply = fmt.Sprintf(msg)
	}
	return nil
}

func stopQuery() {
	if rs, ok := registry[QUERY_RULE_ID]; ok {
		log.Printf("stop the query.")
		(*rs.Topology).Cancel()
		delete(registry, QUERY_RULE_ID)
	}
}

/**
 * qid is not currently used.
 */
func (t *Server) GetQueryResult(qid string, reply *string) error {
	sinks.QR.LastFetch = time.Now()
	sinks.QR.Mux.Lock()
	if len(sinks.QR.Results) > 0 {
		*reply = strings.Join(sinks.QR.Results, "")
		sinks.QR.Results = make([]string, 10)
	} else {
		*reply = ""
	}
	sinks.QR.Mux.Unlock()
	return nil
}


func (t *Server) Stream(stream string, reply *string) error{
	content, err := processors.NewStreamProcessor(stream, path.Join(path.Dir(dataDir), "stream")).Exec()
	if err != nil {
		fmt.Printf("stream command error: %s\n", err)
		return err
	} else {
		for _, c := range content{
			*reply = *reply + fmt.Sprintln(c)
		}
	}
	return nil
}

func (t *Server) CreateRule(rule *common.Rule, reply *string) error{
	r, err := processor.ExecCreate(rule.Name, rule.Json)
	if err != nil {
		return fmt.Errorf("create rule error : %s\n", err)
	} else {
		*reply = fmt.Sprintf("rule %s created", rule.Name)
	}
	//Start the rule
	rs, err := t.createRuleState(r)
	if err != nil {
		return err
	}
	err = t.doStartRule(rs)
	if err != nil {
		return err
	}
	return nil
}

func (t *Server) createRuleState(rule *xstream.Rule) (*RuleState, error){
	if tp, err := processor.ExecInitRule(rule); err != nil{
		return nil, err
	}else{
		rs := &RuleState{
			Name: rule.Id,
			Topology: tp,
			Triggered: true,
		}
		registry[rule.Id] = rs
		return rs, nil
	}
}

func (t *Server) GetStatusRule(name string, reply *string) error{
	if rs, ok := registry[name]; ok{
		if !rs.Triggered {
			*reply = "stopped: canceled manually"
			return nil
		}
		c := (*rs.Topology).GetContext()
		if c != nil{
			err := c.Err()
			switch err{
			case nil:
				*reply = "running"
			case context.Canceled:
				*reply = "stopped: canceled by error"
			case context.DeadlineExceeded:
				*reply = "stopped: deadline exceed"
			default:
				*reply = "stopped: unknown reason"
			}
		}else{
			*reply = "stopped: no context found"
		}
	}else{
		return fmt.Errorf("rule %s not found", name)
	}
	return nil
}

func (t *Server) StartRule(name string, reply *string) error{
	var rs *RuleState
	rs, ok := registry[name]
	if !ok{
		r, err := processor.GetRuleByName(name)
		if err != nil{
			return err
		}
		rs, err = t.createRuleState(r)
		if err != nil {
			return err
		}
	}
	err := t.doStartRule(rs)
	if err != nil{
		return err
	}
	*reply = fmt.Sprintf("rule %s started", name)
	return nil
}

func (t *Server) doStartRule(rs *RuleState) error{
	rs.Triggered = true
	go func() {
		tp := rs.Topology
		select {
		case err := <-tp.Open():
			log.Println(err)
			tp.Cancel()
		}
	}()
	return nil
}

func (t *Server) StopRule(name string, reply *string) error{
	if rs, ok := registry[name]; ok{
		(*rs.Topology).Cancel()
		rs.Triggered = false
		*reply = fmt.Sprintf("rule %s stopped", name)
	}else{
		*reply = fmt.Sprintf("rule %s not found", name)
	}
	return nil
}

func (t *Server) RestartRule(name string, reply *string) error{
	err := t.StopRule(name, reply)
	if err != nil{
		return err
	}
	err = t.StartRule(name, reply)
	if err != nil{
		return err
	}
	*reply = fmt.Sprintf("rule %s restarted", name)
	return nil
}

func (t *Server) DescRule(name string, reply *string) error{
	r, err := processor.ExecDesc(name)
	if err != nil {
		return fmt.Errorf("desc rule error : %s\n", err)
	} else {
		*reply = r
	}
	return nil
}

func (t *Server) ShowRules(_ int, reply *string) error{
	r, err := processor.ExecShow()
	if err != nil {
		return fmt.Errorf("show rule error : %s\n", err)
	} else {
		*reply = r
	}
	return nil
}

func (t *Server) DropRule(name string, reply *string) error{
	r, err := processor.ExecDrop(name)
	if err != nil {
		return fmt.Errorf("drop rule error : %s\n", err)
	} else {
		err := t.StopRule(name, reply)
		if err != nil{
			return err
		}
	}
	*reply = r
	return nil
}

func init(){
	var err error
	dataDir, err = common.GetDataLoc()
	if err != nil {
		log.Panic(err)
	}else{
		log.Infof("db location is %s", dataDir)
	}

	processor = processors.NewRuleProcessor(path.Dir(dataDir))
	registry = make(RuleRegistry)

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			if _, ok := registry[QUERY_RULE_ID]; !ok {
				continue
			}

			n := time.Now()
			w := 10 * time.Second
			if v := n.Sub(sinks.QR.LastFetch); v >= w {
				log.Printf("The client seems no longer fetch the query result, stop the query now.")
				stopQuery()
			}
		}
		//defer ticker.Stop()
	}()
}

func main() {
	server := new(Server)
	//Start rules
	if rules, err := processor.GetAllRules(); err != nil{
		log.Infof("Start rules error: %s", err)
	}else{
		log.Info("Starting rules")
		var reply string
		for _, rule := range rules{
			err = server.StartRule(rule, &reply)
			if err != nil {
				log.Info(err)
			}else{
				log.Info(reply)
			}
		}
	}

	//Start server
	err := rpc.Register(server)
	if err != nil {
		log.Fatal("Format of service Server isn't correct. ", err)
	}
	// Register a HTTP handler
	rpc.HandleHTTP()
	// Listen to TPC connections on port 1234
	listener, e := net.Listen("tcp", fmt.Sprintf(":%d", common.Config.Port))
	if e != nil {
		log.Fatal("Listen error: ", e)
	}
	msg := fmt.Sprintf("Serving Rule server on port %d", common.Config.Port)
	log.Info(msg)
	fmt.Println(msg)
	// Start accept incoming HTTP connections
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("Error serving: ", err)
	}
}
