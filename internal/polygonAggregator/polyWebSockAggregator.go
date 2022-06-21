package polygonAggregator

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type pwsAggregator struct{}

var PWSAggregator = new(pwsAggregator)

func(pwsAggregator) OrchestrateEverything() {
  messageOut := make(chan string)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	u := url.URL{Scheme: "wss", Host: "delayed.polygon.io", Path: "/stocks"}
	log.Printf("connecting to %s", u.String())
	c, resp, err := websocket.DefaultDialer.Dial(u.String(), http.Header{"X-Api-Key": []string{"KZAVCTdHJFMIJ07FU3dTgwqLkuSd2jQE"}});
	if err != nil {
		log.Printf("handshake failed with status %d", resp.StatusCode)
		log.Fatal("dial:", err)
	}

	//When the program closes close the connection
	defer c.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)
			if strings.Contains(string(message), "Connected Successfully"){
				log.Printf("Send Sub Details: %s", message)
				fmt.Println("RESPONSE IS: ", resp)
				messageOut <- "{'action':'auth','params':'KZAVCTdHJFMIJ07FU3dTgwqLkuSd2jQE'}" 
			}
		}

	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {

			case <-done:
				return
			case m := <-messageOut:
				log.Printf("Send Message %s", m)
				err := c.WriteMessage(websocket.TextMessage, []byte(m))
				if err != nil {
					log.Println("write:", err)
					return
				}
			case t := <-ticker.C:
				err := c.WriteMessage(websocket.TextMessage, []byte(t.String()))
				if err != nil {
					log.Println("write:", err)
					return
				}
			case <-interrupt:
				log.Println("interrupt")
				// Cleanly close the connection by sending a close message and then
				// waiting (with timeout) for the server to close the connection.
				err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				if err != nil {
					log.Println("write close:", err)
					return
				}
			select {
				case <-done:
				case <-time.After(time.Second):
			}
			return
		}
	}
 
}
