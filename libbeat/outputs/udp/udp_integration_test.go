// +build !integration

package udp

import (
	"fmt"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/idxmgmt"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/outest"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

const (
	minPort     = 9100
	maxPort     = 9200
	bindAddress = "127.0.0.1"
)

func MakeTestUDPServerWithRandomPort() *UDPServer {
	rand.Seed(time.Now().UnixNano())
	server := MakeTestUDPServer(fmt.Sprintf("%v:%v", bindAddress, rand.Intn(maxPort-minPort+1)+minPort))
	return server
}

func TestWAFRuleViolationCount(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
formats:
  shared_lb_store:
    format: storelbwaf,ipaddress={{require .client}} count=1 violations={{len .alerts}}`
	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp":1607010460,"method":"GET","uri":"\/","id":"99e8a949270d92d20b09","ngx":{"host":"1.2.3.4","request_uri":"\/","request_id":""},"client":"1.2.3.4","alerts":[{"msg":"No valid Accept header","id":21003},{"msg":"No valid User-Agent header","id":21006},{"match":1,"msg":"Host header contains an IP address","id":21010},{"logdata":6,"match":6,"msg":"Request score greater than score threshold","id":99001}]}`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 1 {
		t.Fatalf("should have received one message and instead received %v", len(messages))
	}
	expected := `storelbwaf,ipaddress=1.2.3.4 count=1 violations=4`
	if messages[0] != expected {
		t.Fatalf("should have received message `%v` and instead received `%v`", expected, messages[0])
	}
}


func TestManualSetOfExpirationLimitAndEpochTimeEvaluationWithEntryWithinExpirationWindow(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: true
expiration_limit: 5m
formats:
  shared_lb_store:
    timeProperty: timestamp
    timeFormatUseEpoch: true
    format: storelbwaf,ipaddress={{require .client}} count=1
    processURL: request_uri`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	entryTime := time.Now().Unix()
	entryTimeString := strconv.FormatInt(entryTime, 10)

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp":` + entryTimeString + `,"method":"GET","uri":"\/","id":"99e8a949270d92d20b09","ngx":{"host":"1.2.3.4","request_uri":"\/","request_id":""},"client":"1.2.3.4","alerts":[{"msg":"No valid Accept header","id":21003},{"msg":"No valid User-Agent header","id":21006},{"match":1,"msg":"Host header contains an IP address","id":21010},{"logdata":6,"match":6,"msg":"Request score greater than score threshold","id":99001}]}`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 1 {
		t.Fatalf("should have received one messages as the entry is within the default time limit %v", len(messages))
	}
}

func TestManualSetOfExpirationLimitAndEpochTimeEvaluation(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: true
expiration_limit: 5m
formats:
  shared_lb_store:
    timeProperty: timestamp
    timeFormatUseEpoch: true
    format: storelbwaf,ipaddress={{require .client}} count=1
    processURL: request_uri`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp":1607010460,"method":"GET","uri":"\/","id":"99e8a949270d92d20b09","ngx":{"host":"1.2.3.4","request_uri":"\/","request_id":""},"client":"1.2.3.4","alerts":[{"msg":"No valid Accept header","id":21003},{"msg":"No valid User-Agent header","id":21006},{"match":1,"msg":"Host header contains an IP address","id":21010},{"logdata":6,"match":6,"msg":"Request score greater than score threshold","id":99001}]}`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 0 {
		t.Fatalf("should have received zero messages as the entry is older than the default time limit %v", len(messages))
	}
}

func TestManualSetOfExpirationLimitAndInvalidTimeFormat(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: true
expiration_limit: 5m
formats:
  shared_lb_store:
    timeProperty: timestamp
    timeFormat: 2006_01_02
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	formattedNowTime := time.Now().Format(rfc3339)

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "` + formattedNowTime + `", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 0 {
		t.Fatalf("should have received zero messages as the time format does not match the supplied event data %v", len(messages))
	}
}

func TestManualSetOfExpirationLimitAndEntryBeyondTheLimit(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: true
expiration_limit: 5m
formats:
  shared_lb_store:
    timeProperty: timestamp
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	formattedNowTime := time.Now().Add(-time.Minute * 10).Format(rfc3339)

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "` + formattedNowTime + `", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 0 {
		t.Fatalf("should have received zero messages as the only message exceeds the default expiration time and instead we received %v", len(messages))
	}
}

func TestManualSetOfExpirationLimitAndEntryWithinTheLimit(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: true
expiration_limit: 5m
formats:
  shared_lb_store:
    timeProperty: timestamp
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	formattedNowTime := time.Now().Format(rfc3339)

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "` + formattedNowTime + `", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 1 {
		t.Fatalf("should have received one message and received %v", len(messages))
	}
}

func TestExpiredEntriesWithValidFormatTimeKey(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: true
formats:
  shared_lb_store:
    timeProperty: timestamp
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ and (eq .upstream "apiproxy") (eq .remote_addr "2.3.4.5") }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 0 {
		t.Fatalf("should have received zero messages as the only message exceeds the default expiration time and instead we received %v", len(messages))
	}
}

func TestExpiredEntriesWithInvalidFormatTimeKey(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: true
formats:
  shared_lb_store:
    timeProperty: time
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ and (eq .upstream "apiproxy") (eq .remote_addr "2.3.4.5") }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 1 {
		t.Fatalf("should have received one entry because the time format was invalid and instead we received %v", len(messages))
	}
}

func TestExpiredEntries(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: true
formats:
  shared_lb_store:
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ and (eq .upstream "apiproxy") (eq .remote_addr "2.3.4.5") }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 0 {
		t.Fatalf("should have received zero messages as the only message exceeds the default expiration time and instead we received %v", len(messages))
	}
}

func TestDoubleQualifyingCondition(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  storelb:
    format: 'storelb,storeid={{require .store_id}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1'
    processURL: request_uri
    conditions: '{{ inputsContain .filebeatInputTags "storelb" }}'
  storelbwaf:
    format: 'storelbwaf,uri="{{require (influxEscape .ngx.request_uri)}}",host={{require .ngx.host}},client={{require .client}} count=1'
    processURL: request_uri
    conditions: '{{ inputsContain .filebeatInputTags "storelbwaf" }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"tags":    []string{"storelb"},
				"type":    "udp",
				"message": `{"timestamp": "2020-12-02T00:00:01+00:00", "content_type": "", "server_name": "store.otherteststore.com", "server_port": "80", "remote_addr": "1.2.3.4", "request_method": "GET", "remote_user": "", "request_uri": "/", "status": 301, "http_status_code": 301, "body_bytes_sent": 120, "nginx_version": "1.15.8", "http_referer": "https://t.co/ouzu9W50Uh?amp=1", "request_time": 0.000, "http_user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Mobile/15E148 Safari/604.1", "upstream_cache_status": "", "upstream_addr": "169.254.1.1:4140", "upstream_status": "301", "upstream_response_time": "0.000", "request_id": "", "scheme": "http", "server_protocol": "HTTP/1.1", "ssl_cipher": "", "ssl_protocol": "", "request_completion": "OK", "bc_datacenter": "us-central1", "ssl_server_name": "" , "connecting_proxy": "", "connecting_proxy_ip": "", "fornax_anonymousId": "", "store_id": "123456", "waf_exec_time": "174"}`,
			},
		}, beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"tags":    []string{"storelbwaf"},
				"type":    "udp",
				"message": `{"timestamp":1606867201,"method":"POST","uri":"\/events\/trigger-visit-event","id":"88e515b1862f30e32bda","ngx":{"host":"www.teststore.com","request_uri":"\/events\/trigger-visit-event","request_id":"","http_user_agent":"Mozilla\/5.0 (Linux; Android 6.0.1; Nexus 5X Build\/MMB29P) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/85.0.4183.140 Mobile Safari\/537.36 (compatible; Googlebot\/2.1; +http:\/\/www.google.com\/bot.html)"},"client":"1.2.3.4","alerts":[{"match":"\/events\/trigger-visit-event","msg":"Block GoogleBot from trigger-visit-event","id":91005}]}`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	firstExpected := `storelb,storeid=123456,requesturi="/",ipaddress=1.2.3.4 count=1`
	secondExpected := `storelbwaf,uri="/events/trigger-visit-event",host=www.teststore.com,client=1.2.3.4 count=1`

	if len(messages) != 2 {
		t.Fatalf("received %v and should have received %v entries", len(messages), 2)
	}
	if messages[0] != firstExpected {
		t.Fatalf("should have received `%v`, instead got `%v`", firstExpected, messages[0])
	}
	if messages[1] != secondExpected {
		t.Fatalf("should have received `%v`, instead got `%v`", secondExpected, messages[1])
	}
}

func TestTagPresenceAndCondition(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  storelb:
    format: 'storelb,storeid={{require .store_id}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1'
    processURL: request_uri
    conditions: '{{ inputsContain .filebeatInputTags "storelb" }}'
  storelbwaf:
    format: 'storelbwaf,uri="{{require (influxEscape .ngx.request_uri)}}",host={{require .ngx.host}},client={{require .client}} count=1'
    processURL: request_uri
    conditions: '{{ false }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"tags":    []string{"storelb"},
				"type":    "udp",
				"message": `{"timestamp": "2020-12-02T00:00:01+00:00", "content_type": "", "server_name": "store.otherteststore.com", "server_port": "80", "remote_addr": "1.2.3.4", "request_method": "GET", "remote_user": "", "request_uri": "/", "status": 301, "http_status_code": 301, "body_bytes_sent": 120, "nginx_version": "1.15.8", "http_referer": "https://t.co/ouzu9W50Uh?amp=1", "request_time": 0.000, "http_user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Mobile/15E148 Safari/604.1", "upstream_cache_status": "", "upstream_addr": "169.254.1.1:4140", "upstream_status": "301", "upstream_response_time": "0.000", "request_id": "", "scheme": "http", "server_protocol": "HTTP/1.1", "ssl_cipher": "", "ssl_protocol": "", "request_completion": "OK", "bc_datacenter": "us-central1", "ssl_server_name": "" , "connecting_proxy": "", "connecting_proxy_ip": "", "fornax_anonymousId": "", "store_id": "123456", "waf_exec_time": "174"}`,
			},
		}, beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"tags":    []string{"storelbwaf"},
				"type":    "udp",
				"message": `{"timestamp":1606867201,"method":"POST","uri":"\/events\/trigger-visit-event","id":"88e515b1862f30e32bda","ngx":{"host":"www.teststore.com","request_uri":"\/events\/trigger-visit-event","request_id":"","http_user_agent":"Mozilla\/5.0 (Linux; Android 6.0.1; Nexus 5X Build\/MMB29P) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/85.0.4183.140 Mobile Safari\/537.36 (compatible; Googlebot\/2.1; +http:\/\/www.google.com\/bot.html)"},"client":"1.2.3.4","alerts":[{"match":"\/events\/trigger-visit-event","msg":"Block GoogleBot from trigger-visit-event","id":91005}]}`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	expected := `storelb,storeid=123456,requesturi="/",ipaddress=1.2.3.4 count=1`

	if len(messages) != 1 {
		t.Fatalf("received %v and should have received %v entries", len(messages), 1)
	}
	if messages[0] != expected {
		t.Fatalf("should have received `%v`, instead got `%v`", expected, messages[0])
	}
}

func TestComplexConditions(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  shared_lb_store:
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ and (eq .upstream "apiproxy") (eq .remote_addr "2.3.4.5") }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	expected := `sharedlbstore,storehash=abcxyz123,requesturi="/stores/abcxyz123/v3/catalog/products/11185/images",ipaddress=2.3.4.5 count=1`
	if messages[0] != expected {
		t.Fatalf("should have received `%v`, instead got `%v`", expected, messages)
	}
}

func TestMissingRequiredProperty(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  shared_lb_store:
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .missing_remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ eq .upstream "apiproxy" }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 0 {
		t.Fatalf("length of messages returned is non zero and should be zero")
	}
}

func TestMultiFormatSortOrderByMapKey(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  shared_lb_store:
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ eq .upstream "apiproxy" }}'
  shared_lb:
    format: sharedlb,requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	firstExpected := `sharedlb,requesturi="/stores/abcxyz123/v3/catalog/products/11185/images",ipaddress=2.3.4.5 count=1`
	secondExpected := `sharedlbstore,storehash=abcxyz123,requesturi="/stores/abcxyz123/v3/catalog/products/11185/images",ipaddress=2.3.4.5 count=1`

	if len(messages) != 2 {
		t.Fatalf("length of messages returned is non zero and should be two")
	}
	if messages[0] != firstExpected {
		t.Fatalf("should have received `%v`, instead got `%v`", firstExpected, messages[0])
	}
	if messages[1] != secondExpected {
		t.Fatalf("should have received `%v`, instead got `%v`", secondExpected, messages[1])
	}
}

func TestMultiEventsButOnlyOneValidBasedOnCondition(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  shared_lb_store:
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ eq .upstream "apiproxy" }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-11T19:02:05+00:00", "server_name": "blaze-api.bigcommerce.net", "upstream": "blaze-api", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "OPTIONS", "request_uri": "/activity/1001059703?tz=America%2FLos_Angeles", "http_status_code": 200, "body_bytes_sent": 31, "bytes_sent": 612, "bytes_received": 605, "http_referer": "https://store-abcxyz123.mybigcommerce.com/", "request_time": 4.012, "http_user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 4.008, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-11T19:02:06+00:00", "server_name": "login.bigcommerce.com", "upstream": "auth", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/session/heartbeat", "http_status_code": 200, "body_bytes_sent": 42, "bytes_sent": 701, "bytes_received": 1986, "http_referer": "https://login.bigcommerce.com/login", "request_time": 0.000, "http_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36", "upstream_addr": "169.254.1.1:4140", "upstream_status": "200", "upstream_response_time": 0.000, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	expected := `sharedlbstore,storehash=abcxyz123,requesturi="/stores/abcxyz123/v3/catalog/products/11185/images",ipaddress=2.3.4.5 count=1`

	if len(messages) != 1 {
		t.Fatalf("length of messages returned is non zero and should be one")
	}
	if messages[0] != expected {
		t.Fatalf("should have received `%v`, instead got `%v`", expected, messages)
	}
}

func TestLiteralFalseConditionalYieldingNoResults(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  shared_lb_store:
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ false }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-11T19:02:05+00:00", "server_name": "blaze-api.bigcommerce.net", "upstream": "blaze-api", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "OPTIONS", "request_uri": "/activity/1001059703?tz=America%2FLos_Angeles", "http_status_code": 200, "body_bytes_sent": 31, "bytes_sent": 612, "bytes_received": 605, "http_referer": "https://store-abcxyz123.mybigcommerce.com/", "request_time": 4.012, "http_user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 4.008, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-11T19:02:06+00:00", "server_name": "login.bigcommerce.com", "upstream": "auth", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/session/heartbeat", "http_status_code": 200, "body_bytes_sent": 42, "bytes_sent": 701, "bytes_received": 1986, "http_referer": "https://login.bigcommerce.com/login", "request_time": 0.000, "http_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36", "upstream_addr": "169.254.1.1:4140", "upstream_status": "200", "upstream_response_time": 0.000, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	if len(messages) != 0 {
		t.Fatalf("should have received no entries and instead got %v", len(messages))
	}
}

func TestLiteralTrueConditionalYieldingNoResults(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  shared_lb_store:
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ true }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-03T08:41:00-08:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products/11185/images", "http_status_code": 200, "body_bytes_sent": 997, "bytes_sent": 1410, "bytes_received": 296, "http_referer": "", "request_time": 0.221, "http_user_agent": "GuzzleHttp/6.2.1 curl/7.51.0 PHP/7.0.21", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.220, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "123456", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-11T19:02:05+00:00", "server_name": "blaze-api.bigcommerce.net", "upstream": "blaze-api", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "OPTIONS", "request_uri": "/activity/1001059703?tz=America%2FLos_Angeles", "http_status_code": 200, "body_bytes_sent": 31, "bytes_sent": 612, "bytes_received": 605, "http_referer": "https://store-abcxyz123.mybigcommerce.com/", "request_time": 4.012, "http_user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 4.008, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-11T19:02:06+00:00", "server_name": "login.bigcommerce.com", "upstream": "auth", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/session/heartbeat", "http_status_code": 200, "body_bytes_sent": 42, "bytes_sent": 701, "bytes_received": 1986, "http_referer": "https://login.bigcommerce.com/login", "request_time": 0.000, "http_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36", "upstream_addr": "169.254.1.1:4140", "upstream_status": "200", "upstream_response_time": 0.000, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()

	if len(messages) != 3 {
		t.Fatalf("should have received three entries and instead got %v", len(messages))
	}
}

func TestExcludesUsingConditionals(t *testing.T) {

	server := MakeTestUDPServerWithRandomPort()
	defer server.close()
	go func() {
		server.serve()
	}()

	yamlConfig := `
skip_expired_entries: false
formats:
  shared_lb_store:
    format: sharedlbstore,storehash={{splitGrab .request_uri.Path "/" 2}},requesturi="{{require (influxEscape .request_uri.Path)}}",ipaddress={{require .remote_addr}} count=1
    processURL: request_uri
    conditions: '{{ not (or (inputContains .request_uri.Path "/stores/abcxyz123/v3/catalog/products") (inputContains .remote_addr "2.3.4.5")) }}'`

	testConfig, _ := common.NewConfigWithYAML([]byte(yamlConfig), "test")
	config, _ := common.MergeConfigs(testConfig, server.getHostAndPortConfig())
	client := makeTestUDPOutput(t, config)
	defer client.Close()

	batch := outest.NewBatch(
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-10T00:24:55+00:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products?page=1&limit=100", "http_status_code": 200, "body_bytes_sent": 15932, "bytes_sent": 16345, "bytes_received": 239, "http_referer": "", "request_time": 0.120, "http_user_agent": "", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.120, "upstream_connect_time": 0.004, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "12345", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-10T00:25:35+00:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "POST", "request_uri": "/stores/abcxyz123/v3/catalog/products/10072/options", "http_status_code": 422, "body_bytes_sent": 192, "bytes_sent": 655, "bytes_received": 455, "http_referer": "", "request_time": 0.081, "http_user_agent": "", "upstream_addr": "1.2.3.4:1234", "upstream_status": "422", "upstream_response_time": 0.084, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "12345", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-10T00:27:07+00:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.5", "request_method": "GET", "request_uri": "/stores/abcxyz123/v3/catalog/products?page=12&limit=100", "http_status_code": 200, "body_bytes_sent": 19432, "bytes_sent": 19846, "bytes_received": 240, "http_referer": "", "request_time": 0.073, "http_user_agent": "", "upstream_addr": "1.2.3.4:1234", "upstream_status": "200", "upstream_response_time": 0.072, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "12345", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		},
		beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    "udp",
				"message": `{"timestamp": "2020-11-10T00:27:35+00:00", "server_name": "api.bigcommerce.com", "upstream": "apiproxy", "server_port": "1443", "remote_addr": "2.3.4.6", "request_method": "POST", "request_uri": "/stores/abcxyz123/v3/catalog/products/10070/options", "http_status_code": 422, "body_bytes_sent": 192, "bytes_sent": 655, "bytes_received": 455, "http_referer": "", "request_time": 0.081, "http_user_agent": "", "upstream_addr": "1.2.3.4:1234", "upstream_status": "422", "upstream_response_time": 0.084, "upstream_connect_time": 0.000, "bc_request_id": "", "request_bc_auth_client": "", "bc_auth_client": "", "bc_store_id": "12345", "scheme": "https", "server_protocol": "HTTP/1.1", "ssl_cipher": "ECDHE-RSA-CHACHA20-POLY1305", "ssl_version": "TLSv1.2", "x_client_type": "", "x_client_version": "", "x_plugin_version": "", "x_php_version": "", "x_client_name": "" }`,
			},
		})
	client.Publish(batch)

	// batch is processed async
	time.Sleep(1 * time.Second)
	messages := server.getMessages()
	expected := "sharedlbstore,storehash=abcxyz123,requesturi=\"/stores/abcxyz123/v3/catalog/products/10070/options\",ipaddress=2.3.4.6 count=1"

	if len(messages) != 1 {
		t.Fatalf("should have received one entry and instead got %v", len(messages))
	}
	if messages[0] != expected {
		t.Fatalf("should have received `%v`, instead got `%v`", expected, messages)
	}
}

func makeTestUDPOutput(t *testing.T, cfg interface{}) outputs.Client {
	config, err := common.NewConfigFrom(cfg)
	if err != nil {
		t.Fatal(err)
	}

	info := beat.Info{Beat: "udp"}
	im, _ := idxmgmt.DefaultSupport(nil, info, nil)
	output, err := makeUdpout(im, info, outputs.NewNilObserver(), config)
	if err != nil {
		t.Fatal(err)
	}

	udpOutNetworkClient := output.Clients[0].(outputs.NetworkClient)
	udpOutNetworkClient.Connect()

	return udpOutNetworkClient
}
