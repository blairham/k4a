// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
)

// k4a-mcp reaches k4a-index with its own credential, so the index sees one
// caller — k4a-mcp — whatever person or agent is behind it. --authz-policy
// here is what tells those callers apart: the Guard identifies each one by
// its client certificate or token, and these wrappers check that caller's
// grant before a tool touches the index.

// authorizeCoverage refuses a coverage call for a topic the caller may not
// search: coverage says whether, and how far, a topic is indexed.
func authorizeCoverage(
	authz endpointauth.Authorizer, next mcp.ToolHandlerFor[mcpCoverageInput, mcpCoverageOutput],
) mcp.ToolHandlerFor[mcpCoverageInput, mcpCoverageOutput] {
	return func(ctx context.Context, req *mcp.CallToolRequest, in mcpCoverageInput) (
		*mcp.CallToolResult, mcpCoverageOutput, error,
	) {
		if err := checkTool(ctx, authz, endpointauth.ActionSearch, in.Topic); err != nil {
			return nil, mcpCoverageOutput{}, err
		}
		return next(ctx, req, in)
	}
}

// authorizeSearch refuses a search for a topic the caller may not search. A
// Search of an unfollowed topic makes k4a-index start following it, so for a
// caller who may search but not follow the topic, this asks for coverage
// first and answers "not followed" without the Search that would start one.
func authorizeSearch(
	authz endpointauth.Authorizer, client indexv1.IndexClient,
	next mcp.ToolHandlerFor[mcpSearchInput, mcpSearchOutput],
) mcp.ToolHandlerFor[mcpSearchInput, mcpSearchOutput] {
	return func(ctx context.Context, req *mcp.CallToolRequest, in mcpSearchInput) (
		*mcp.CallToolResult, mcpSearchOutput, error,
	) {
		if err := checkTool(ctx, authz, endpointauth.ActionSearch, in.Topic); err != nil {
			return nil, mcpSearchOutput{}, err
		}
		if authz.Allows(ctx, endpointauth.ActionFollow, in.Topic) {
			return next(ctx, req, in)
		}
		_, cov, err := coverageHandler(client)(ctx, req, mcpCoverageInput{Topic: in.Topic})
		if err != nil {
			return nil, mcpSearchOutput{}, err
		}
		if cov.ServedFromIndex {
			return next(ctx, req, in)
		}
		log.Info("mcp tool: search of an unfollowed topic the caller may not follow; not starting a follow",
			"topic", in.Topic, "caller", endpointauth.GrantFromContext(ctx).Caller())
		return nil, mcpSearchOutput{Topic: in.Topic, Coverage: cov.Coverage, Matches: []mcpMatch{}}, nil
	}
}

// checkTool applies the policy to one tool call. An empty topic is left to the
// handler, which refuses it with its own message.
func checkTool(ctx context.Context, authz endpointauth.Authorizer, action endpointauth.Action, topic string) error {
	if strings.TrimSpace(topic) == "" {
		return nil
	}
	err := authz.Check(ctx, action, topic)
	if err != nil {
		log.Warn("mcp tool: refused by the authorization policy", "action", action.String(), "topic", topic,
			"caller", endpointauth.GrantFromContext(ctx).Caller())
	}
	return err
}
