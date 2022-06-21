package polygonAggregator_test

import (
	"fmt"
	"os"
	"os/signal"
	"testing"
	"time"

	p "github.com/Kaylthomas777/polygonWebSocketAggregator/internal/polygonAggregator"
	"github.com/matryer/is"
	polygonws "github.com/polygon-io/client-go/websocket"
	"github.com/polygon-io/client-go/websocket/models"
	"github.com/sirupsen/logrus"
)

func TestSetUp(t *testing.T) {
	is := is.NewRelaxed(t)
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetFormatter(&logrus.JSONFormatter{})
	c, err := polygonws.New(polygonws.Config{
		APIKey: os.Getenv("POLYGON_API_KEY"),
		Feed:   polygonws.RealTime,
		Market: polygonws.Crypto,
		Log:    log,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	_ = c.Subscribe(polygonws.CryptoTrades, "BTC-USD")
	if err := c.Connect(); err != nil {
		log.Error(err)
		return
	}
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	ticker := time.NewTicker(30 * time.Second)
	stopTicker := time.NewTicker(92 * time.Second)
	defer ticker.Stop()
	defer stopTicker.Stop()
	for {
		select {
		case <-sigint:
			return
		case <-c.Error():
			return
		case <-stopTicker.C:
			is.True(len(p.AllTradesPerWindow) == 3)
			return
		case t := <-ticker.C:
			//Every 30 seconds output aggregate!
			is.True(len(p.CollectedTrades) >= 1)
            fmt.Println("Tick at", t)
			p.PrintOutAggregate(p.MakeAggregate(p.CollectedTrades, t))
			is.True(len(p.AllTradesPerWindow) >= 1)
		case out, more := <-c.Output():
			if !more {
				return
			}
			switch out.(type) {
			case models.CryptoTrade:
				p.CollectCryptoTrades(out)
			}
		}
	}
}

