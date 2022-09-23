package main

import "strings"

// isValidSubscribeTopic returns true if the given topic is a valid subscribe topic
func isValidSubscribeTopic(topic *string) bool {
	// can be "bucket" or "bucket/filename" or "bucket/*" or "*"
	contentsCount := len(strings.Split(*topic, "/"))
	return contentsCount <= 2 && contentsCount >= 1
}

// isValidPublishTopic returns true if the given topic is a valid publish topic
func isValidPublishTopic(topic *string) bool {
	// bucket/filename or bucket
	topic_slice := strings.Split(*topic, "/")

	if len(topic_slice) == 2 {
		return topic_slice[0] != "*" && topic_slice[1] != "*"
	} else if len(topic_slice) == 1 {
		return topic_slice[0] != "*"
	}

	return false
}

// isSubTopic returns true if the given subTopic is a valid
// instance of the given pubTopic
// subTopic can be: bucket, bucket/*,bucket/filename,*, */filename
// pubTopic can be: bucket, bucket/filename
func isSubTopic(subTopic, pubTopic string) bool {
	if subTopic == pubTopic || subTopic == "*" {
		return true
	}
	subTopic_ := strings.Split(subTopic, "/")
	pubTopic_ := strings.Split(pubTopic, "/")

	if len(pubTopic_) == 2 && len(subTopic_) == 2 {
		if subTopic_[0] == pubTopic_[0] && subTopic_[1] == "*" {
			return true
		} else if subTopic_[0] == "*" && subTopic_[1] == pubTopic_[1] {
			return true
		}
	}

	return false
}
