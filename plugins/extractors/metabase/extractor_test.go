//+build integration

package metabase_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/logger"
	"github.com/odpf/meteor/plugins/extractors/metabase"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	fname                  = "meteor"
	lname                  = "metabase"
	collection_name        = "temp_collection_meteor"
	collection_color       = "#ffffb3"
	collection_description = "Temp Collection for Meteor Metabase Extractor"
	dashboard_name         = "random_dashboard"
	dashboard_description  = "some description"
	email                  = "meteorextractortestuser@gmail.com"
	pass                   = "meteor_pass_1234"
	port                   = "3000"
	url                    = "http://localhost:3000"
)

var (
	client = &http.Client{
		Timeout: 2 * time.Second,
	}
	session_id    = ""
	collection_id = 1
	card_id       = 0
	dashboard_id  = 0
)

type responseID struct {
	ID int `json:"id"`
}

type sessionID struct {
	ID string `json:"id"`
}

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository:   "metabase/metabase",
		Tag:          "latest",
		ExposedPorts: []string{port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			port: {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}

	retryFn := func(resource *dockertest.Resource) (err error) {
		res, err := http.Get(url + "/api/health")
		if err != nil {
			return
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("received %d status code", res.StatusCode)
		}
		return
	}

	// Exponential backoff-retry for container to be resy to accept connections
	err, purgeFn := testutils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	if err := setup(); err != nil {
		log.Fatal(err)
	}

	// Run tests
	code := m.Run()

	// Clean tests
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestExtract(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"user_id": "user",
			"host":    url,
		}, make(chan<- interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return dashboard model", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})
		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"user_id":    email,
				"password":   pass,
				"host":       url,
				"session_id": session_id,
			}, extractOut)
			close(extractOut)
			assert.NoError(t, err)
		}()
		var urns []string
		for val := range extractOut {
			urns = append(urns, val.(meta.Dashboard).Urn)
		}
		assert.Equal(t, []string{"metabase.random_dashboard"}, urns)
	})
}

func newExtractor() *metabase.Extractor {
	return metabase.New(
		logger.NewWithWriter("info", ioutil.Discard),
	)
}

func setup() (err error) {
	type responseToken struct {
		Token string `json:"setup-token"`
	}
	var data responseToken
	err = newRequest("GET", url+"/api/session/properties", nil, &data)
	if err != nil {
		return
	}
	setup_token := data.Token
	err = setUser(setup_token)
	if err != nil {
		return
	}
	err = addMockData(session_id)
	if err != nil {
		return
	}
	return
}

func setUser(setup_token string) (err error) {
	values := map[string]interface{}{
		"user": map[string]interface{}{
			"first_name": fname,
			"last_name":  lname,
			"email":      email,
			"password":   pass,
			"site_name":  "Unaffiliated",
		},
		"token": setup_token,
		"prefs": map[string]interface{}{
			"site_name":      "Unaffiliated",
			"allow_tracking": "true",
		},
	}

	jsonValue, err := json.Marshal(values)
	if err != nil {
		return
	}
	var data sessionID
	err = newRequest("POST", url+"/api/setup", bytes.NewBuffer(jsonValue), &data)
	if err != nil {
		return
	}
	session_id = data.ID
	err = getSessionID()
	return
}

func getSessionID() (err error) {
	values := map[string]interface{}{
		"username": email,
		"password": pass,
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return
	}
	var data sessionID
	err = newRequest("POST", url+"/api/session", bytes.NewBuffer(jsonValue), &data)
	if err != nil {
		return
	}
	session_id = data.ID
	return
}

func addMockData(session_id string) (err error) {
	err = addCollection()
	if err != nil {
		return
	}
	err = addDashboard()
	if err != nil {
		return
	}
	return
}

func addCollection() (err error) {
	values := map[string]interface{}{
		"name":        collection_name,
		"color":       collection_color,
		"description": collection_description,
	}

	jsonValue, err := json.Marshal(values)
	if err != nil {
		return
	}
	var data responseID
	err = newRequest("POST", url+"/api/collection", bytes.NewBuffer(jsonValue), &data)
	if err != nil {
		return
	}
	collection_id = data.ID
	return
}

func addDashboard() (err error) {
	values := map[string]interface{}{
		"name":          dashboard_name,
		"description":   dashboard_description,
		"collection_id": collection_id,
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return
	}
	var data responseID
	err = newRequest("POST", url+"/api/dashboard", bytes.NewBuffer(jsonValue), &data)
	if err != nil {
		return
	}
	dashboard_id = data.ID
	err = addCard(dashboard_id)
	if err != nil {
		return
	}
	return
}

func addCard(id int) (err error) {
	values := map[string]interface{}{
		"id": id,
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return
	}
	x := strconv.Itoa(id)
	type response struct {
		ID int `json:"id"`
	}
	var data response
	err = newRequest("POST", url+"/api/dashboard/"+x+"/cards", bytes.NewBuffer(jsonValue), &data)
	if err != nil {
		return
	}
	card_id = data.ID
	return
}

func newRequest(method, url string, body io.Reader, data interface{}) (err error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if session_id != "" {
		req.Header.Set("X-Metabase-Session", session_id)
	}
	res, err := client.Do(req)
	if err != nil {
		return
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &data)
	return
}
