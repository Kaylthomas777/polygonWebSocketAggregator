package polygonAggregator

import (
	"fmt"
	"os"
	"os/signal"
	"sort"
	"time"

	polygonws "github.com/polygon-io/client-go/websocket"
	"github.com/polygon-io/client-go/websocket/models"
	"github.com/sirupsen/logrus"
)

var AllTradesPerWindow = make(map[string][]models.CryptoTrade)
var CollectedTrades []models.CryptoTrade

func Orchestrate(symbol string) {
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

	_ = c.Subscribe(polygonws.CryptoTrades, symbol)
	if err := c.Connect(); err != nil {
		log.Error(err)
		return
	}

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-sigint:
			return
		case <-c.Error():
			return
		case t := <-ticker.C:
			PrintOutAggregate(MakeAggregate(CollectedTrades, t))
		case out, more := <-c.Output():
			if !more {
				return
			}
			switch out.(type) {
			case models.CryptoTrade:
				CollectCryptoTrades(out)
			}
		}
	}
}

func CollectCryptoTrades(trade interface{}) {
	if str, ok := trade.(models.CryptoTrade); ok {
		CollectedTrades = append(CollectedTrades, str)
	}
}

func MakeAggregate(trades []models.CryptoTrade, ticker time.Time) map[string]any {
	result := make(map[string]any)
	openPrice, closePrice := getAggregateOpenClosePrice(trades)
	highPrice, lowPrice := getAggregateHighLowPrice(trades)
	formatedTime := ticker.Format("2006-1-2 03:04:05")
	result["Open Price"] = openPrice
	result["Close Price"] = closePrice
	result["High Price"] = highPrice
	result["Low Price"] = lowPrice
	result["Volume"] = getAggregateVolume(trades)
	result["Start Time"] = formatedTime
	AllTradesPerWindow["TRADE WINDOW" + formatedTime] = CollectedTrades
	CollectedTrades = []models.CryptoTrade{}
	return result
}

func getAggregateOpenClosePrice(trades []models.CryptoTrade) (float64, float64) {
	sortTradesByTimeStamp(trades)
	return trades[0].Price, trades[len(trades)- 1].Price
}

func getAggregateHighLowPrice(trades []models.CryptoTrade) (float64, float64) {
	sortTradesByPrice(trades)
	return trades[0].Price, trades[len(trades) - 1].Price
}

func getAggregateVolume(trades []models.CryptoTrade) int {
	return len(trades)
}

func sortTradesByPrice(trades []models.CryptoTrade) {
	sort.Slice(trades, func(i, j int) bool { 
    return trades[i].Price > trades[j].Price 
	})
}

func sortTradesByTimeStamp(trades []models.CryptoTrade) {
	sort.Slice(trades, func(i, j int) bool { 
    return trades[i].Timestamp < trades[j].Timestamp 
	})
}

func PrintOutAggregate(aggregate map[string]any) {
	t:= aggregate
	s := fmt.Sprintf(
		"%v - open: $%.2f close: $%.2f high: $%.2f low: $%.2f volume: %d",
		 t["Start Time"],  
		 t["Open Price"], 
		 t["Close Price"], 
		 t["High Price"], 
		 t["Low Price"], 
		 t["Volume"])

	fmt.Println(s)
}


