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

var allTradesPerWindow = make(map[string][]models.CryptoTrade)
var collectedTrades []models.CryptoTrade

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
			//fmt.Println("COLLECTED TRADES: ", collectedTrades)
			fmt.Println("ALLTRADESPERWINDOW IS: ", allTradesPerWindow)
			return
		case <-c.Error():
			return
		case t := <-ticker.C:
			//Every 30 seconds output aggregate!
            fmt.Println("Tick at", t)
			printOutAggregate(makeAggregate(collectedTrades, t))
		case out, more := <-c.Output():
			if !more {
				return
			}
			switch out.(type) {
			case models.CryptoTrade:
				collectCryptoTrades(out)
				//log.WithFields(logrus.Fields{"Trades": out}).Info()
			}
		}
	}
}

func collectCryptoTrades(trade interface{}) {
	if str, ok := trade.(models.CryptoTrade); ok {
		collectedTrades = append(collectedTrades, str)
	}
}

func makeAggregate(trades []models.CryptoTrade, ticker time.Time) map[string]any {
	result := make(map[string]any)
	openPrice, closePrice := getAggregateOpenClosePrice(trades)
	highPrice, lowPrice := getAggregateHighLowPrice(trades)
	formatedTime := ticker.Format("2006-1-2 03:04:05")
	//result["Ticket Symbol"] = str.Pair
	result["Open Price"] = openPrice
	result["Close Price"] = closePrice
	result["High Price"] = highPrice
	result["Low Price"] = lowPrice
	result["Volume"] = getAggregateVolume(trades)
	result["Start Time"] = formatedTime
	allTradesPerWindow["TRADE WINDOW" + formatedTime] = collectedTrades
	collectedTrades = []models.CryptoTrade{}
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

// func miliSecondsToTime(miliSeconds int) time.Time {
// 	tm := time.Unix(miliSeconds, 0)
// 	return tm
// }

func printOutAggregate(aggregate map[string]any) {
	t:= aggregate
	s := fmt.Sprintf(
		"Start: %v - open: %.2f close: %.2f high: %.2f low: %.2f volume: %d \n",
		 t["Start Time"],  
		 t["Open Price"], 
		 t["Close Price"], 
		 t["High Price"], 
		 t["Low Price"], 
		 t["Volume"])

	fmt.Println(s)
}


