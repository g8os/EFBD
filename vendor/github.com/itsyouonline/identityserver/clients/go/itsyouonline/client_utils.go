package itsyouonline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func encodeBody(data interface{}) (io.Reader, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

// do HTTP request with request body
func (c Itsyouonline) doReqWithBody(method, urlStr string, data interface{}, headers, queryParams map[string]interface{}) (*http.Response, error) {
	body, err := encodeBody(data)
	if err != nil {
		return nil, err
	}
	return c.doReq(method, urlStr, body, headers, queryParams)
}

// do http request without request body
func (c Itsyouonline) doReqNoBody(method, urlStr string, headers, queryParams map[string]interface{}) (*http.Response, error) {
	return c.doReq(method, urlStr, nil, headers, queryParams)
}

func (c Itsyouonline) doReq(method, urlStr string, body io.Reader, headers, queryParams map[string]interface{}) (*http.Response, error) {
	// create the request
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = buildQueryString(req, queryParams)

	if c.AuthHeader != "" {
		req.Header.Set("Authorization", c.AuthHeader)
	}
	for k, v := range headers {
		req.Header.Set(k, fmt.Sprintf("%v", v))
	}

	return c.client.Do(req)
}

func buildQueryString(req *http.Request, qs map[string]interface{}) string {
	q := req.URL.Query()

	for k, v := range qs {
		q.Add(k, fmt.Sprintf("%v", v))
	}
	return q.Encode()
}

//Date represent RFC3399 date
type Date time.Time

//MarshalJSON override marshalJSON
func (t *Date) MarshalJSON() ([]byte, error) {
	return []byte(time.Time(*t).Format(`"` + time.RFC3339 + `"`)), nil
}

//MarshalText override marshalText
func (t *Date) MarshalText() ([]byte, error) {
	return []byte(time.Time(*t).Format(`"` + time.RFC3339 + `"`)), nil
}

//UnmarshalJSON override unmarshalJSON
func (t *Date) UnmarshalJSON(b []byte) error {
	ts, err := time.Parse(`"`+time.RFC3339+`"`, string(b))
	if err != nil {
		return err
	}

	*t = Date(ts)
	return nil
}

//UnmarshalText override unmarshalText
func (t *Date) UnmarshalText(b []byte) error {
	ts, err := time.Parse(`"`+time.RFC3339+`"`, string(b))
	if err != nil {
		return err
	}

	*t = Date(ts)
	return nil
}

func (t *Date) String() string {
	return time.Time(*t).String()
}
