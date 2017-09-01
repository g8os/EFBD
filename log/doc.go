/*Package log defines a complete logging API and is to be used for all 0-Disk info/error logging purposes.

Broadcast

Broadcast is a function that wraps 0-log specifically for logging errors in JSON format to the 0-Orchestrator.

It takes a MessageStatus, MessageSubject and an interface for additional data that needs to be sent.
These values are used to form the Message struct which will be send over to zerolog.Log to be logged.

MessageStatus is a status code that represents the status of the MessageSubject.

MessageSubject represents who the status applies to (storage(ardb), etcd, tlog).

No error is returned as this would be used as a last resort to notify the 0-Orchestrator to intervene.

More information about Broadcast can be found in the 0-Disk docs: https://github.com/zero-os/0-Disk/blob/master/docs/log.md#broadcasting-to-0-orchestrator
and more information about 0-log can be found at the 0-log github page: https://github.com/zero-os/0-log
*/
package log
