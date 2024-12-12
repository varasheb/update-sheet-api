package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fabrikiot/wsmqttrt/wsmqttrtpuller"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

var CoprocSettingMap = make(map[string]string)

func main() {
	getallmergeData()

	// if err := fetchProtectedSheet(); err != nil {
	// 	log.Fatalf("Error accessing protected sheet: %v", err)
	// }
	if err := updateSheetWithCoprocSettings(CoprocSettingMap); err != nil {
		log.Fatalf("Error updating sheet: %v", err)
	}
}
func authenticateServiceAccount() (*sheets.Service, error) {
	keyFile := "./sheetapicall-80dcffbd485d.json"

	credentials, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file: %v", err)
	}

	ctx := context.Background()
	sheetsService, err := sheets.NewService(ctx, option.WithCredentialsJSON(credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create Sheets service: %v", err)
	}

	return sheetsService, nil
}

func updateSheetWithCoprocSettings(coprocSettings map[string]string) error {
	// spreadsheetId := "1-wJlg02eDoymK46okM00pwoVKz5sYF1uCQaN4QJZDCw"  TEST ID
	// deviceDataRange := "Sheet1!A1"
	// timestampRange := "Sheet1!D1"
	// clearRange := "Sheet1"
	spreadsheetId := "1sNMfT2txXqjWWrLk9rjIaLAB0ZM5zadvvYEx_S7Lzk8"
	// "Daily Firmware Update Status!A1"
	deviceDataRange := "Daily Firmware Update Status!A1"
	timestampRange := "Daily Firmware Update Status!D1"
	clearRange := "Daily Firmware Update Status!A1"
	var values [][]interface{}
	// values = append(values, []interface{}{"Device ID", "Coproc Setting"})
	for deviceID, setting := range coprocSettings {
		values = append(values, []interface{}{deviceID, setting})
	}

	currentTime := time.Now().Format("2006-01-02 15:04:05")
	timestampValues := [][]interface{}{
		{"Last Updated", currentTime},
	}

	sheetsService, err := authenticateServiceAccount()
	if err != nil {
		return fmt.Errorf("authentication failed: %v", err)
	}
	clearRequest := &sheets.ClearValuesRequest{}
	_, err = sheetsService.Spreadsheets.Values.Clear(spreadsheetId, clearRange, clearRequest).Do()
	if err != nil {
		return fmt.Errorf("failed to clear sheet data: %v", err)
	}
	valueRange := &sheets.ValueRange{
		Values: values,
	}
	_, err = sheetsService.Spreadsheets.Values.Update(spreadsheetId, deviceDataRange, valueRange).
		ValueInputOption("RAW").Do()
	if err != nil {
		return fmt.Errorf("failed to update device data: %v", err)
	}

	timestampRangeValues := &sheets.ValueRange{
		Values: timestampValues,
	}
	_, err = sheetsService.Spreadsheets.Values.Update(spreadsheetId, timestampRange, timestampRangeValues).
		ValueInputOption("RAW").Do()
	if err != nil {
		return fmt.Errorf("failed to update timestamp: %v", err)
	}

	fmt.Println("Sheet updated successfully with Coproc Settings and Last Updated timestamp.")
	return nil
}

func getallmergeData() {
	fmt.Println("Config updates script started...")

	wspulleropts := wsmqttrtpuller.NewWsMqttRtPullerOpts("dmt1.intellicar.in", 11884)
	wg := &sync.WaitGroup{}

	stoppedflag := uint32(0)
	var once sync.Once

	statecallback := &wsmqttrtpuller.WsMqttRtPullerStateCallback{
		Started: func() {
			// Signal that the puller has started
			fmt.Println("Puller started")
		},
		Stopped: func() {
			once.Do(func() {
				atomic.StoreUint32(&stoppedflag, 1)
				fmt.Println("Puller stopped")
			})
		},
	}

	subscribecallback := func(topic []byte, issubscribe bool, isok bool) {
		if isok {
			fmt.Printf("Subscribed to topic: %s\n", string(topic))
		} else {
			fmt.Printf("Failed to subscribe to topic: %s\n", string(topic))
		}
	}

	msgcallback := func(topic []byte, payload []byte) {
		topics := strings.Split(string(topic), "/")
		deviceid := topics[len(topics)-1]
		topictype := topics[len(topics)-2]

		if topictype == "coprocstatus" {
			coproc_payload := make(map[string]interface{})

			_, exists := CoprocSettingMap[deviceid]
			if exists {
				return
			}

			if err := json.Unmarshal(payload, &coproc_payload); err != nil {
				log.Println("Error unmarshalling payload:", err)
				return
			}

			if coprocStatusInfo, ok := coproc_payload["coprocStatusInfo"].(map[string]interface{}); ok {
				if hexValue, ok := coprocStatusInfo["4"].(string); ok {
					asciiValue := hexToASCII(hexValue)
					CoprocSettingMap[deviceid] = asciiValue
				} else {
					log.Printf("Device ID: %s, Coproc Setting: Key '4' not found\n", deviceid)
				}
			} else {
				log.Printf("Device ID: %s, Coproc Status Info: Not available\n", deviceid)
			}
		}
	}

	wspuller := wsmqttrtpuller.NewWsMqttRtPuller(wspulleropts, statecallback, msgcallback)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wspuller.Start()
		for atomic.LoadUint32(&stoppedflag) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}()

	Topics := []string{"/intellicar/layer5/coprocstatus/"}
	subscribedTopics := make(map[string]bool)
	for _, eachtopic := range Topics {
		fullTopic := eachtopic + "+"
		if !subscribedTopics[fullTopic] {
			subscribedTopics[fullTopic] = true
			wspuller.Subscribe([]byte(fullTopic), subscribecallback)
		}
	}

	ossigch := make(chan os.Signal, 1)
	signal.Notify(ossigch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-ossigch:
		fmt.Println("Interrupt signal received, stopping...")
	case <-time.After(1 * time.Minute):
		fmt.Println("Timeout reached, stopping...")
	}

	wspuller.Stop()
	wg.Wait()

	fmt.Println("Collected Coproc Settings map :", len(CoprocSettingMap))
	// for deviceID, setting := range CoprocSettingMap {
	// 	fmt.Println(deviceID, ",", setting)
	// }

	fmt.Println("Config updates script ended...")
}

func hexToASCII(hexStr string) string {
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return getcoproc(hexStr)
	}
	asciiStr := string(bytes)

	return getcoproc(asciiStr)
}

func getcoproc(cset string) string {
	cset = strings.TrimRight(cset, "\u0000")
	cset = strings.Split(cset, ",")[0]
	return cset
}

func fetchProtectedSheet() error {
	spreadsheetId := "1-wJlg02eDoymK46okM00pwoVKz5sYF1uCQaN4QJZDCw"
	readRange := "Sheet1"

	sheetsService, err := authenticateServiceAccount()
	if err != nil {
		return fmt.Errorf("authentication failed: %v", err)
	}

	resp, err := sheetsService.Spreadsheets.Values.Get(spreadsheetId, readRange).Do()
	if err != nil {
		return fmt.Errorf("failed to fetch data: %v", err)
	}

	fmt.Println("Sheet Data:")
	for _, row := range resp.Values {
		fmt.Println(row)
	}

	return nil
}
