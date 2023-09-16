package http

import (
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/config"
)

func testHttp() {
	logrus.Warn("HTTP")
}

type HttpServer struct {
	httpConfig config.HttpConfig
	reqCh      chan interface{}
}

type SetTask struct {
	Key   string
	Value string
	ResCh chan interface{}
}

type GetTask struct {
	Key   string
	ResCh chan interface{}
}

func CreateHttpServer(httpConfig config.HttpConfig, reqCh chan interface{}) HttpServer {
	return HttpServer{httpConfig: httpConfig, reqCh: reqCh}
}

// Define a setHandler function
func (s HttpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	logrus.Debugf("http handler path = \"%s\" key = \"%s\" value: \"%s\" ", r.URL.Path, key, value)
	resCh := make(chan interface{})
	timeout := time.After(time.Second * time.Duration(s.httpConfig.DefaultTimeout))
	select {
	case s.reqCh <- SetTask{Key: key, Value: value, ResCh: resCh}:
		//
	case <-timeout:
		http.Error(w, "server busy", http.StatusInternalServerError)
		return
	}

	rawRes := <-resCh
	switch res := rawRes.(type) {
	case string:
		fmt.Fprintf(w, res)
	case error:
		http.Error(w, res.Error(), http.StatusInternalServerError)
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
}

func (s HttpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	logrus.Debugf("http handler path = \"%s\" key = \"%s\"", r.URL.Path, key)
	resCh := make(chan interface{})

	timeout := time.After(time.Second * time.Duration(s.httpConfig.DefaultTimeout))
	select {
	case s.reqCh <- GetTask{Key: key, ResCh: resCh}:
		//
	case <-timeout:
		http.Error(w, "server busy", http.StatusInternalServerError)
		return
	}

	rawRes := <-resCh
	switch res := rawRes.(type) {
	case string:
		fmt.Fprintf(w, res)
	case error:
		http.Error(w, res.Error(), http.StatusInternalServerError)
	default:
		logrus.Panicf("http unkown res type: %v", reflect.TypeOf(res))
	}
}

func (s HttpServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "healthy")
}

func (s HttpServer) StartHttp() {
	logrus.Info("starting http server")
	http.HandleFunc("/set", s.setHandler)
	http.HandleFunc("/get", s.getHandler)
	http.HandleFunc("/health", s.healthHandler)
	http.ListenAndServe(":8080", nil)
}
