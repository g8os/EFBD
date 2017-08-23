# Creatign a log file

All logs produced by the identityserver are directed to standard output. If desired, file redirection can be used to produce a more permanent logfile.
Should you only want to log the more important messages, the logs can be filtered before being redirected.

**Example command**
```
./identityserver -d | tee /dev/tty | grep --line-buffered -A 2 -B 5 -e ".*level=\(warning\|error\|fatal\|panic\|info msg=\"SMS:\).*" >> log.txt
```
***Command Explanation***

```./identityserver -d ```

In the current directory, start the ```identityserver``` executable. In this case, start it in debug mode (```-d``` flag)

```tee /dev/tty```

This is a small hack, it allows us to pipe the output to ```grep``` and display it in the terminal

```grep --line-buffered -A 2 -B 5 -e ".*level=\(warning\|error\|fatal\|panic\|info msg=\"SMS:\).*"```

In this example, we`ve used ```grep``` to parse the logs and decides which ones we want to write to the logfile.
```--line-buffered``` is required here, to make sure the logs are written to the file correctly. ```-A 2 -B 5```
tells ```grep``` to also write the 5 previous and 2 following log messages to the logfile, to provide us with some context.
```-e ".*level=\(warning\|error\|fatal\|panic\|info msg=\"SMS:\).*"``` tells ```grep``` to match logs that contain either
'level=warning', 'level=error', 'level=fatal', 'level=panic' or 'level=info msg="SMS:'.
The last case will make sure the logs of an outgoing sms message are also stored.

```>> log.txt```

makes sure the output from ```grep``` gets appended to the file ```log.txt``` in the current directory. If the file doesn't exist, it will be created when the command is executed.
