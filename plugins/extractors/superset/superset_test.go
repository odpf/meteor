//go:build plugins
// +build plugins

package superset_test

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
	"testing"
	"time"

	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/test/utils"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/superset"
	"github.com/odpf/meteor/test/mocks"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	user           = "admin"
	pass           = "admin"
	port           = "9999"
	provider       = "db"
	dashboardTitle = "random dashboard"
	mockChart      = "random chart"
	urnScope       = "test-superset"
)

var (
	client = &http.Client{
		Timeout: 4 * time.Second,
	}
	accessToken = ""
	csrfToken   = ""
	chartID     = 0
	dashboardID = 0
	host        = "http://localhost:" + port
)

type responseToken struct {
	JwtToken string `json:"access_token"`
}

type securityToken struct {
	CsrfToken string `json:"result"`
}

func TestMain(m *testing.M) {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// setup test
	opts := dockertest.RunOptions{
		Repository:   "apache/superset",
		Tag:          "6b136c2bc9a6c9756e5319b045e3c42da06243cb",
		ExposedPorts: []string{"8088"},
		Mounts: []string{
			fmt.Sprintf("%s/localConfig:/app/pythonpath:rw", pwd),
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8088": {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}

	retryFn := func(resource *dockertest.Resource) (err error) {
		res, err := http.Get(host + "/health")
		if err != nil {
			return
		}
		if res.StatusCode != http.StatusOK {
			err = fmt.Errorf("Response failed with status code: %d", res.StatusCode)
			return
		}

		var stdout bytes.Buffer
		_, err = resource.Exec(
			[]string{
				"superset", "fab", "create-admin",
				"--username", user,
				"--firstname", "Superset",
				"--lastname", "Admin",
				"--email", "admin@superset.com",
				"--password", pass,
			},
			dockertest.ExecOptions{StdOut: &stdout},
		)

		_, err = resource.Exec(
			[]string{
				"superset", "db", "upgrade",
			},
			dockertest.ExecOptions{StdOut: &stdout},
		)

		_, err = resource.Exec(
			[]string{
				"superset", "load_examples", "--load-test-data",
			},
			dockertest.ExecOptions{StdOut: &stdout},
		)

		_, err = resource.Exec(
			[]string{
				"superset", "init",
			},
			dockertest.ExecOptions{StdOut: &stdout},
		)
		return
	}

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	purgeFn, err := utils.CreateContainer(opts, retryFn)
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

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := superset.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"user_id": "user",
				"host":    host,
			}})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		ctx := context.TODO()
		extr := superset.New(utils.Logger)
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"username": user,
				"password": pass,
				"host":     host,
				"provider": provider,
			}})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		var urns []string
		for _, record := range emitter.Get() {
			dashboard := record.Data().(*assetsv1beta1.Dashboard)
			urns = append(urns, dashboard.Resource.Urn)
		}
		assert.Equal(t, 4, len(urns))
	})
}

// setup sets up the test environment
func setup() (err error) {
	err = checkUser()
	if err != nil {
		return
	}
	return
}

// checkUser checks if the user exists
// if so, it login through the given credentials
func checkUser() (err error) {
	payload := map[string]interface{}{
		"password": pass,
		"provider": provider,
		"username": user,
	}
	var data responseToken
	if err = makeRequest("POST", host+"/api/v1/security/login", payload, &data); err != nil {
		return
	}
	accessToken = data.JwtToken
	if err = setCsrfToken(); err != nil {
		return
	}
	return
}

// setCsrfToken sets the CSRF token for the next request
func setCsrfToken() (err error) {
	var data securityToken
	// got the token from here
	err = makeRequest("GET", host+"/api/v1/security/csrf_token", nil, &data)
	if err != nil {
		return
	}
	csrfToken = data.CsrfToken
	return
}

// makeRequest helper function to avoid rewriting a request
func makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode the payload JSON: %w", err)
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	var bearer = "Bearer " + accessToken
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", bearer)
	req.Header.Set("X-CSRFToken", csrfToken)
	req.Header.Set("Referer", url)

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to generate response: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		bodyBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		bodyString := string(bodyBytes)
		return fmt.Errorf("response failed with status code: %d and body: %s", res.StatusCode, bodyString)
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("failed to parse: %s, err: %w", string(b), err)
	}
	return
}
