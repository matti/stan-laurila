package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

func usage() {
	println("wat")
	os.Exit(1)
}

func main() {
	done := make(chan bool, 1)
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log.Println("got signal", <-sigs)
		done <- false
	}()

	var (
		mode      string
		clusterID string
		clientID  string
		subject   string
		URL       string
		natsConn  *nats.Conn
		stanConn  stan.Conn
	)

	flag.StringVar(&URL, "server", stan.DefaultNatsURL, "The nats server URLs (separated by comma)")

	flag.Usage = usage
	flag.Parse()

	if len(flag.Args()) < 3 {
		usage()
	}

	mode = flag.Arg(0)
	clusterID = flag.Arg(1)
	clientID = flag.Arg(2)
	subject = flag.Arg(3)

	natsOpts := []nats.Option{
		nats.Name("stanistan"),
		nats.Timeout(3 * time.Second),
		nats.NoReconnect(),
		nats.MaxPingsOutstanding(1),
		nats.PingInterval(1 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Println("nats disconnected")
			done <- false
		}),
	}
	if v, err := nats.Connect(URL, natsOpts...); err != nil {
		log.Fatalln("connect nats", err, URL)
	} else {
		natsConn = v
	}

	log.Println("nats connect ok")
	defer natsConn.Close()

	log.Println("connect stan", clusterID, clientID)
	if v, err := stan.Connect(clusterID, clientID, stan.NatsConn(natsConn)); err != nil {
		log.Fatalln("connect stan", err)
	} else {
		stanConn = v
	}
	defer stanConn.Close()

	switch mode {
	case "publish":
		var msg []byte
		reader := bufio.NewReader(os.Stdin)
		msg, isPrefix, err := reader.ReadLine()
		if err != nil {
			log.Fatalln("stdin read", err)
		}
		if isPrefix {
			log.Fatalln("stdin read", "too long")
		}

		if err := stanConn.Publish(subject, msg); err != nil {
			log.Fatalln("publish", err)
		}
		log.Println("published", msg)
		done <- true
	case "subscribe":
		mcb := func(msg *stan.Msg) {
			log.Println("msg", msg.Subject, string(msg.Data))
			println(string(msg.Data))
			if err := msg.Ack(); err != nil {
				log.Println("failed to acknowledge", err)
			}
		}

		stanOpts := []stan.SubscriptionOption{
			stan.DeliverAllAvailable(),
			stan.SetManualAckMode(),
		}
		stanConn.QueueSubscribe(subject, "default", mcb, stanOpts...)
	default:
		usage()
	}
	<-done
}
