package config

import (
	"fmt"
	"os"
)

type Conf struct {
	RMQ *RMQ
	SAP *SAP
}

func NewConf() *Conf {
	return &Conf{
		RMQ: newRMQ(),
		SAP: newSAP(),
	}
}

func newRMQ() *RMQ {
	return &RMQ{
		user:  os.Getenv("RMQ_USER"),
		pass:  os.Getenv("RMQ_PASS"),
		addr:  os.Getenv("RMQ_ADDRESS"),
		port:  os.Getenv("RMQ_PORT"),
		vhost: os.Getenv("RMQ_VHOST"),
		queueFrom: []string{
			os.Getenv("RMQ_QUEUE_FROM"),
		},
		queueTo: []string{
			os.Getenv("RMQ_QUEUE_TO"),
		},
	}
}

type RMQ struct {
	user  string
	pass  string
	addr  string
	port  string
	vhost string

	queueFrom []string
	queueTo   []string
}

func (c *RMQ) URL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/%s", c.user, c.pass, c.addr, c.port, c.vhost)
}

func (c *RMQ) QueueFrom() []string {
	return c.queueFrom
}
func (c *RMQ) QueueTo() []string {
	return c.queueTo
}

type SAP struct {
	baseURL string
}

func newSAP() *SAP {
	return &SAP{
		baseURL: os.Getenv("SAP_API_BASE_URL"),
	}
}
func (c *SAP) BaseURL() string {
	return c.baseURL
}
