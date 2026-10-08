// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package style

import "testing"

func TestViewName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		view ViewType
	}{
		{name: "Topics", view: ViewTopics, want: "Topics"},
		{name: "TopicDetail", view: ViewTopicDetail, want: "Topic Detail"},
		{name: "TopicConfig", view: ViewTopicConfig, want: "Topic Config"},
		{name: "Groups", view: ViewGroups, want: "Groups"},
		{name: "GroupDetail", view: ViewGroupDetail, want: "Group Detail"},
		{name: "Cluster", view: ViewCluster, want: "Cluster"},
		{name: "Messages", view: ViewMessages, want: "Messages"},
		{name: "MessageDetail", view: ViewMessageDetail, want: "Message"},
		{name: "Produce", view: ViewProduce, want: "Produce"},
		{name: "CreateTopic", view: ViewCreateTopic, want: "Create Topic"},
		{name: "CreateACL", view: ViewCreateACL, want: "Create ACL"},
		{name: "Context", view: ViewContext, want: "Contexts"},
		{name: "ACLs", view: ViewACLs, want: "ACLs"},
		{name: "ResetOffsets", view: ViewResetOffsets, want: "Reset Offsets"},
		{name: "BrokerDetail", view: ViewBrokerDetail, want: "Broker Detail"},
		{name: "Unknown", view: ViewType(99), want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ViewName(tt.view); got != tt.want {
				t.Errorf("ViewName(%d) = %q, want %q", tt.view, got, tt.want)
			}
		})
	}
}

func TestViewResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		view ViewType
	}{
		{name: "Topics", view: ViewTopics, want: "topic"},
		{name: "TopicDetail", view: ViewTopicDetail, want: "partition"},
		{name: "TopicConfig", view: ViewTopicConfig, want: "config"},
		{name: "Groups", view: ViewGroups, want: "group"},
		{name: "GroupDetail", view: ViewGroupDetail, want: "offset"},
		{name: "Cluster", view: ViewCluster, want: "broker"},
		{name: "Messages", view: ViewMessages, want: "message"},
		{name: "MessageDetail", view: ViewMessageDetail, want: "message"},
		{name: "Produce", view: ViewProduce, want: "produce"},
		{name: "CreateTopic", view: ViewCreateTopic, want: "topic"},
		{name: "CreateACL", view: ViewCreateACL, want: "acl"},
		{name: "Context", view: ViewContext, want: "context"},
		{name: "ACLs", view: ViewACLs, want: "acl"},
		{name: "ResetOffsets", view: ViewResetOffsets, want: "offset"},
		{name: "BrokerDetail", view: ViewBrokerDetail, want: "config"},
		{name: "Unknown", view: ViewType(99), want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ViewResource(tt.view); got != tt.want {
				t.Errorf("ViewResource(%d) = %q, want %q", tt.view, got, tt.want)
			}
		})
	}
}

func TestViewNameAllNonEmpty(t *testing.T) {
	t.Parallel()

	knownViews := []ViewType{
		ViewTopics, ViewTopicDetail, ViewTopicConfig,
		ViewGroups, ViewGroupDetail, ViewCluster,
		ViewMessages, ViewMessageDetail, ViewProduce,
		ViewCreateTopic, ViewCreateACL, ViewContext,
		ViewACLs, ViewResetOffsets, ViewBrokerDetail,
	}

	for _, v := range knownViews {
		name := ViewName(v)
		if name == "" || name == "Unknown" {
			t.Errorf("ViewName(%d) = %q, expected a real name", v, name)
		}
	}
}

func TestViewResourceAllNonEmpty(t *testing.T) {
	t.Parallel()

	knownViews := []ViewType{
		ViewTopics, ViewTopicDetail, ViewTopicConfig,
		ViewGroups, ViewGroupDetail, ViewCluster,
		ViewMessages, ViewMessageDetail, ViewProduce,
		ViewCreateTopic, ViewCreateACL, ViewContext,
		ViewACLs, ViewResetOffsets, ViewBrokerDetail,
	}

	for _, v := range knownViews {
		resource := ViewResource(v)
		if resource == "" {
			t.Errorf("ViewResource(%d) returned empty", v)
		}
	}
}
