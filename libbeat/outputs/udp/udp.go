package udp

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/transport"
	"github.com/elastic/beats/libbeat/publisher"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
)

var influxLineProtoEscapeRegex = regexp.MustCompile("[ ,='\"]")

const (
	influxLineProtoReplacement = "\\${0}"
	rfc3339                    = "2006-01-02T15:04:05-07:00"
	udp                        = "udp"
	inputTagsJsonKey           = "filebeatInputTags"
)

func init() {
	outputs.RegisterType(udp, makeUdpout)
}

type udpOutput struct {
	*transport.Client
	formats            []*Format
	beat               beat.Info
	observer           outputs.Observer
	logger             *logp.Logger
	skipExpiredEntries bool
	expirationLimit    time.Duration
}

type formatByName []*Format

func (s formatByName) Len() int {
	return len(s)
}
func (s formatByName) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s formatByName) Less(i, j int) bool {
	return len(s[i].name) < len(s[j].name)
}

type Format struct {
	name           string
	conditions     *template.Template
	conditionsBody string
	template       *template.Template
	templateBody   string
	processURL     string
	useEpoch       bool
	timeProperty   string
	timeFormat     string
}

// makeUdpout instantiates a new UDP output instance.
func makeUdpout(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	config := defaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}

	// build the specific format for transforming log events as payloads to the UDP endpoint
	funcMap := make(template.FuncMap)
	funcMap["split"] = strings.Split
	funcMap["require"] = require
	funcMap["pathEscape"] = url.PathEscape
	funcMap["splitGrab"] = splitGrab
	funcMap["takeFirst"] = takeFirst
	funcMap["influxEscape"] = influxEscape
	funcMap["inputsContain"] = inputsContain
	funcMap["inputContains"] = inputContains
	funcMap["exists"] = exists
	funcMap["join"] = strings.Join

	logger := logp.NewLogger(udp)

	formats := make([]*Format, 0)

	for name, formatConfig := range config.Formats {

		var err error
		var conditionsTemplate *template.Template = nil

		if len(formatConfig.Conditions) > 0 {
			conditionsTemplate, err = template.New("").Funcs(funcMap).Parse(formatConfig.Conditions)
			if err != nil {
				return outputs.Fail(err)
			}
		}

		template, err := template.New("").Funcs(funcMap).Parse(formatConfig.Format)
		if err != nil {
			return outputs.Fail(err)
		}

		timeFormat := rfc3339
		if len(formatConfig.TimeFormat) > 0 {
			timeFormat = formatConfig.TimeFormat
		}

		timeProperty := "timestamp"
		if len(formatConfig.TimeProperty) > 0 {
			timeProperty = formatConfig.TimeProperty
		}

		format := &Format{
			name:           name,
			template:       template,
			templateBody:   formatConfig.Format,
			processURL:     formatConfig.ProcessURL,
			conditions:     conditionsTemplate,
			conditionsBody: formatConfig.Conditions,
			useEpoch:       formatConfig.TimeFormatUseEpoch,
			timeFormat:     timeFormat,
			timeProperty:   timeProperty,
		}

		formats = append(formats, format)
	}

	transportConfig := &transport.Config{
		Timeout: config.Timeout,
		Stats:   observer,
	}

	sort.Sort(formatByName(formats))

	clients := make([]outputs.NetworkClient, len(config.Hosts))
	for index, host := range config.Hosts {
		conn, err := transport.NewClient(transportConfig, udp, host, config.Port)
		if err != nil {
			return outputs.Fail(err)
		}

		udpOut := &udpOutput{
			formats:            formats,
			beat:               beat,
			observer:           observer,
			Client:             conn,
			logger:             logger,
			skipExpiredEntries: config.SkipExpiredEntries,
			expirationLimit:    config.ExpirationLimit,
		}
		clients[index] = newBackoffClient(udpOut, config.Backoff.Init, config.Backoff.Max)
	}

	return outputs.SuccessNet(false, config.BulkMaxSize, config.MaxRetries, clients)
}

// Implement Outputer
func (out *udpOutput) Close() error {
	return out.Client.Close()
}

func (out *udpOutput) Publish(
	batch publisher.Batch,
) error {
	observer := out.observer

	dropped := 0
	acked := 0

	observer.NewBatch(len(batch.Events()))

	for _, event := range batch.Events() {

		// get the message portion of the event (this is the body of the input read)
		message, err := event.Content.Fields.GetValue("message")
		if err != nil {
			out.logger.Debugf("unable to get message from event '%v'", event)
		}

		// type conversion
		coercedMessage := message.(string)

		// unmarshal the JSON payload to a dictionary
		var data map[string]interface{}
		err = json.Unmarshal([]byte(coercedMessage), &data)

		if err != nil {
			out.logger.Debugf("failed to unmarshal message JSON message for event: '%v', error: %v", coercedMessage, err)
			dropped++
			continue
		}

		// get the tags off the event
		tags, err := event.Content.Fields.GetValue("tags")
		if err == nil {
			tagsArray := tags.([]string)
			data[inputTagsJsonKey] = tagsArray
		} else {
			data[inputTagsJsonKey] = make([]string, 0)
		}

		messages := make([][]byte, 0)

		for formatIndex, format := range out.formats {

			// if a property is configured with the name of "process URL", convert it to a proper URL
			urlParam, urlParamExists := data[format.processURL]
			if urlParamExists && formatIndex == 0 {
				url, err := url.Parse(urlParam.(string))
				if err != nil {
					out.logger.Debugf("failed to parse URL: %v", urlParam)
				}
				data[format.processURL] = url
			}

			if format.conditions != nil {
				buffer := new(bytes.Buffer)
				format.conditions.Execute(buffer, data)
				bufferString := buffer.String()
				conditionsResult, err := strconv.ParseBool(bufferString)
				if err != nil {
					out.logger.Debugf("dropping format: '%v' for message: '%v' as it produced an error: '%v'", format.templateBody, coercedMessage, err)
					continue
				}
				if !conditionsResult {
					out.logger.Debugf("dropping format: '%v' for message: '%v' as it did not pass with a result of: '%v'", format.templateBody, coercedMessage, bufferString)
					continue
				}
			}

			// drop the event if configured to drop expired events
			if out.skipExpiredEntries {
				var parsedTime time.Time
				eventMessageTime, eventMessageKeyExists := data[format.timeProperty]
				if eventMessageKeyExists {
					if format.useEpoch {
						parsedTime = time.Unix(int64(int(eventMessageTime.(float64))), 0)
					} else {
						parsedTime, err = time.Parse(format.timeFormat, eventMessageTime.(string))
						if err != nil {
							out.logger.Errorf("dropping event '%v' with time key: '%v', value: '%v', encountered a time format parse error: %v", event, format.timeProperty, eventMessageTime, err)
							dropped++
							continue
						}
					}
					out.logger.Debugf("time now: %v, event time: %v, event payload time: %v, difference in hours: %v", time.Now(), event.Content.Timestamp, parsedTime, time.Now().Sub(parsedTime).Hours())
					if time.Now().Sub(parsedTime) > out.expirationLimit {
						out.logger.Debugf("event '%v' has expired", event)
						dropped++
						continue
					}
				}
			}

			// run the JSON through the configured template
			buffer := new(bytes.Buffer)
			err = format.template.Execute(buffer, data)

			if err != nil {
				out.logger.Debugf("failed to render the event: '%v' through the configured template: %v, error: %v", coercedMessage, format.templateBody, err)
				dropped++
				continue
			}

			messages = append(messages, buffer.Bytes())
		}

		for _, message := range messages {
			out.logger.Debugf("writing message: '%v'", string(message))
			_, err = out.Client.Write(message)
			if err != nil {
				out.logger.Errorf("writing event to UDP connection failed with error: %v", err)

				// notify our observer
				observer.Failed(1)
				observer.WriteError(err)
				observer.Acked(acked)
				observer.Dropped(dropped)

				// tell batch to retry
				batch.RetryEvents([]publisher.Event{event})
				return err
			}
		}

		acked++
	}

	observer.Dropped(dropped)
	observer.Acked(acked)
	batch.ACK()

	return nil
}

func (out *udpOutput) String() string {
	return "udp()"
}

func influxEscape(str string) string {
	return influxLineProtoEscapeRegex.ReplaceAllString(str, influxLineProtoReplacement)
}

func takeFirst(first, second string) string {
	if len(first) == 0 {
		return second
	}
	return first
}

func require(val string) (string, error) {
	if len(val) == 0 {
		return "", errors.New("required value not set")
	}
	return val, nil
}

func inputContains(input string, matches ...string) bool {
	for _, match := range matches {
		if input == match {
			return true
		}
	}
	return false
}

func inputsContain(input []string, match string) bool {
	for _, input := range input {
		if input == match {
			return true
		}
	}
	return false
}

func exists(s string) bool {
	return len(s) > 0
}

func splitGrab(input, delimiter string, index int) (string, error) {
	splits := strings.Split(input, delimiter)
	if index > len(splits)-1 {
		return "", errors.New("slice index out of bounds")
	}
	return splits[index], nil
}
