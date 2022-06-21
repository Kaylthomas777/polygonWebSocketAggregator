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

var allTrades []map[string][]models.CryptoTrade
var allAggregates []map[string]any
var collectedTrades []models.CryptoTrade

func OrchestrateEverythin() {
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
	defer ticker.Stop()
	for {
		select {
		case <-sigint:
			fmt.Println("COLLECTED TRADES: ", collectedTrades)
			return
		case <-c.Error():
			return
		case t := <-ticker.C:
			//Every 30 seconds output aggregate!
            fmt.Println("Tick at", t)
			printOutAggregate(makeAggregate(collectedTrades))
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

//func getAggregateStartTime(){}


func makeAggregate(trades []models.CryptoTrade) map[string]any {
	result := make(map[string]any)
	openPrice, closePrice, startTime := getAggregateOpenClosePrice(trades)
	highPrice, lowPrice := getAggregateHighLowPrice(trades)
	formatedTime := miliSecondsToTime(startTime).Format("2006-1-2 03:04:05")
	//result["Ticket Symbol"] = str.Pair
	result["Open Price"] = openPrice
	result["Close Price"] = closePrice
	result["High Price"] = highPrice
	result["Low Price"] = lowPrice
	result["Volume"] = getAggregateVolume(trades)
	result["Start Time"] = formatedTime
	allAggregates = append(allAggregates, result)
	collectedTrades = []models.CryptoTrade{}
	
	//allTrades[formatedTime] = collectedTrades
	return result
}

func getAggregateOpenClosePrice(trades []models.CryptoTrade) (float64, float64, int64) {
	sortTradesByTimeStamp(trades)
	return trades[0].Price, trades[len(trades)- 1].Price, trades[0].Timestamp
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

func miliSecondsToTime(miliSeconds int64) time.Time {
	tm := time.Unix(miliSeconds, 0)
	return tm
}




func printOutAggregate(aggregate map[string]any) {
	t:= aggregate
	//13:00:00 - open: $79.00, close: $80.00, high: $80.50, low: $78.00, volume: 200
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


// func transformAggregate(aggregate interface{}) map[string]any {
// 	result := make(map[string]any) 
// 	if str, ok := aggregate.(models.CryptoTrade); ok {
// 		result["Ticket Symbol"] = str.Pair
// 		result["Open Price"] = str.Open
// 		result["Close Price"] = str.Close
// 		result["High Price"] = str.High
// 		result["Low Price"] = str.Low
// 		result["Volume"] = str.Volume
// 		result["Start Time"] = miliSecondsToTime(str.StartTimestamp).Format("2006-1-2 03:04:05")
// 		result["End Time"] = miliSecondsToTime(str.EndTimestamp).Format("2006-1-2 03:04:05")
// 	} 
// 	return result
// }

