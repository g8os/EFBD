# 0-log [![Build Status](https://travis-ci.org/zero-os/0-log.svg?branch=master)](https://travis-ci.org/zero-os/0-log)

A package that prints out log messages in a format the [0-Core][core] log monitor can read.

## Format

The logs printed out by this package follow the specs specified in the [logging documentation][monitorFormat] and are formatted as follows:

Single-line:
```
<loglevel>::<message>
```

Multi-line:
```
<loglevel>:::
<message line 1>
<message line 2>
:::
```

## Supported log levels

The package currently supports following log levels from the [0-Core log monitor][monitorLevels]:

* 1: stdout
* 2: stderr
* 10: statistics/monitoring message
* 20: result message, JSON

## Usage

```go
package main

import "github.com/zero-os/0-log"

func main() {
    // print to the zero-os stdout (single-line)
    zerolog.Log(zerolog.LevelStdout, "Hello world")
    // output: 1::Hello world 

    // print to the zero-os stderr
    zerolog.Log(zerolog.LevelStderr, "Hello world")
    // output: 2::Hello world 

    // print a multi-line message
    // Log() detects if a message is multi-lined and applies the multi-line format if so
    zerolog.Log(zerolog.LevelStdout, "Hello\nworld")
    /* output: 
    1:::
    Hello
    world
    :::
    */

    // print a statistics message
    msgStat := zerolog.MsgStatistics{
        // statistic key (required)
        Key: "somekey",
        // statistic value (float)
        // (required)
        Value: 123.456,
        // statistic aggregation strategy (average or differentiate)
        // (required)
        Operation: zerolog.AggregationAverages,
        // statistics tags map (optional)
        Tags: zerolog.MetricTags{
            "foo":   "bar",
            "hello": "world",
        },
    }
    zerolog.Log(zerolog.LevelStatistics, msgStat)
    // output: 10::somekey:123.456000|A|foo=bar,hello=world

    // print a json result message
    type testStruct struct {
        Message string `json:"message"`
    }
    zerolog.Log(zerolog.LevelJSON, testStruct{
        Message: "Hello world",
    })
    // output: 20::{"message":"Hello world"}
}
```

[core]: https://github.com/zero-os/0-core
[monitorFormat]: https://github.com/zero-os/0-core/blob/master/docs/monitoring/logging.md#message-format
[monitorLevels]: https://github.com/zero-os/0-core/blob/master/docs/monitoring/logging.md#log-levels