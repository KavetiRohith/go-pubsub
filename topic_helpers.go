package main

import "strings"

// isValidSubscribeTopic returns true if the given topic is a valid subscribe topic
func isValidSubscribeTopic(topic *string) bool {
	// can be "sitename/*" or "sitename/bucket" or "sitename/bucket/*" or "sitename/bucket/filename" or "sitename/*/filename"
	contentsCount := len(strings.Split(*topic, "/"))
	return contentsCount <= 3 && contentsCount > 1
}

// isValidPublishTopic returns true if the given topic is a valid publish topic
func isValidPublishTopic(topic *string) bool {
	// sitename/bucket/filename or sitename/bucket
	topic_slice := strings.Split(*topic, "/")

	if len(topic_slice) == 3 {
		return topic_slice[0] != "*" && topic_slice[1] != "*" && topic_slice[2] != "*"
	} else if len(topic_slice) == 2 {
		return topic_slice[0] != "*" && topic_slice[1] != "*"
	}

	return false
}

// isSubTopic returns true if the given subTopic is a valid
// instance of the given pubTopic
// subTopic can be: sitename/bucket, sitename/bucket/*, sitename/bucket/filename, sitename/*, sitename/*/filename
// pubTopic can be: sitename/bucket, sitename/bucket/filename
func isSubTopic(subTopic, pubTopic string) bool {
	// covers sitename/bucket/filename, sitename/bucket subtopic cases
	if subTopic == pubTopic {
		return true
	}
	sub_topic_contents := strings.Split(subTopic, "/")
	pub_topic_contents := strings.Split(pubTopic, "/")

	// covers sitename/* case
	if len(sub_topic_contents) == 2 && sub_topic_contents[0] == pub_topic_contents[0] && sub_topic_contents[1] == "*" {
		return true
	}

	// covers sitename/bucket/*, sitename/*/filename case
	if len(sub_topic_contents) == 3 && sub_topic_contents[0] == pub_topic_contents[0] {
		if sub_topic_contents[1] == pub_topic_contents[1] && sub_topic_contents[2] == "*" {
			return true
		}

		if sub_topic_contents[1] == "*" && sub_topic_contents[2] == pub_topic_contents[2] {
			return true
		}
	}

	return false
}
