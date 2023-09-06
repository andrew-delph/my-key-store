package http

import (
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus"
)

func testHttp() {
	logrus.Warn("HTTP")
}

type HttpServer struct {
	reqCh chan interface{}
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

func NewHttpServer(ch chan interface{}) HttpServer {
	return HttpServer{reqCh: ch}
}

// Define a setHandler function
func (s HttpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	logrus.Debugf("http handler path = \"%s\" key = \"%s\" value: \"%s\" ", r.URL.Path, key, value)
	resCh := make(chan interface{})
	s.reqCh <- SetTask{Key: key, Value: value, ResCh: resCh}
	res := <-resCh
	fmt.Fprintf(w, "%s", res)
}

func (s HttpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	logrus.Debugf("http handler path = \"%s\" key = \"%s\"", r.URL.Path, key)
	resCh := make(chan interface{})
	s.reqCh <- GetTask{Key: key, ResCh: resCh}
	res := <-resCh
	fmt.Fprintf(w, "%s", res)
}

func (s HttpServer) StartHttp() {
	logrus.Info("starting http server")
	http.HandleFunc("/set", s.setHandler)
	http.HandleFunc("/get", s.getHandler)
	http.ListenAndServe(":8080", nil)
}
