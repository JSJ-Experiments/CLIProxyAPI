package management

import (
	"bufio"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtimeevents"
)

func TestGetRuntimeEventsStreamsPublishedEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := &Handler{}
	router.GET("/events", h.GetRuntimeEvents)
	server := httptest.NewServer(router)
	defer server.Close()

	client := server.Client()
	client.Timeout = 2 * time.Second
	resp, err := client.Get(server.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}

	runtimeevents.Publish(runtimeevents.Event{
		Type:         "codex.capacity",
		Status:       "retrying",
		Model:        "gpt-test",
		RetryAttempt: 2,
		RetryLimit:   3,
		WaitMS:       1000,
	})

	reader := bufio.NewReader(resp.Body)
	var data string
	for i := 0; i < 4; i++ {
		line, errRead := reader.ReadString('\n')
		if errRead != nil {
			t.Fatal(errRead)
		}
		if strings.HasPrefix(line, "data: ") {
			data = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			break
		}
	}
	if data == "" {
		t.Fatal("runtime event stream did not include a data frame")
	}
	var event runtimeevents.Event
	if errDecode := json.Unmarshal([]byte(data), &event); errDecode != nil {
		t.Fatal(errDecode)
	}
	if event.Type != "codex.capacity" || event.RetryAttempt != 2 || event.RetryLimit != 3 || event.WaitMS != 1000 {
		t.Fatalf("unexpected streamed event: %#v", event)
	}
}
