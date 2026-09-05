// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package server

import (
	"context"
	"sync"

	"golang.org/x/tools/gopls/internal/protocol"
	"golang.org/x/tools/internal/event"
)

// This file implements the server state protocol
// (https://github.com/tagawa0525/lsp-det): the experimental/serverState
// request and the experimental/serverStateChanged notification, which tell
// a client whether the answers to workspace-wide requests (references,
// definition, workspace symbols, call hierarchy, rename, ...) can be trusted.
//
// readiness is "initializing" until the first workspace folder has been
// loaded, "indexing" while the initial load of any folder is in flight
// (also when folders are added), and "ready" otherwise. gopls reloads
// packages lazily and synchronously within each request (Snapshot.awaitLoaded),
// so a request answered while "ready" is complete and reflects every change
// the client has sent: the server declares coverage and freshness.
//
// health is "error" while the last load of a folder failed or while a
// critical workspace load error is shown ("Error loading workspace"), and
// "ok" otherwise.

// serverStateProtocol tracks the values reported by the server state protocol.
type serverStateProtocol struct {
	mu          sync.Mutex
	clientWants bool // the client declared the experimental.serverState capability
	loading     int  // folders whose initial load is in flight
	loaded      bool // at least one initial load has completed
	failed      map[protocol.DocumentURI]string
	critical    string // the current "Error loading workspace" message, if any
	last        *protocol.ServerState
}

// serverStateCapability is the experimental server capability declared in
// the InitializeResult. The guarantees name what is missing from the ideal:
// workspace/symbol is capped at maxSymbols (golang/workspace_symbol.go) and
// the cap is declared; created, changed and deleted files are all folded in
// synchronously when the client reports them.
var serverStateCapability = map[string]any{
	"coverage": map[string]any{
		"scope":      "workspace",
		"incomplete": map[string]int{"workspace/symbol": 100},
	},
	"freshness": map[string]any{
		"fileChanges": []string{"Created", "Changed", "Deleted"},
	},
}

// clientWantsServerState reports whether the client declared
// experimental.serverState in its capabilities.
func clientWantsServerState(caps protocol.ClientCapabilities) bool {
	exp, ok := caps.Experimental.(map[string]any)
	if !ok {
		return false
	}
	v, _ := exp["serverState"].(bool)
	return v
}

func (t *serverStateProtocol) currentLocked() protocol.ServerState {
	state := protocol.ServerState{Health: "ok", Readiness: "ready"}
	switch {
	case t.loading > 0:
		state.Readiness = "indexing"
	case !t.loaded:
		state.Readiness = "initializing"
	}
	if t.critical != "" {
		state.Health = "error"
		state.Message = t.critical
	} else if len(t.failed) > 0 {
		state.Health = "error"
		for _, msg := range t.failed {
			state.Message = msg
			break
		}
	}
	return state
}

// current returns the current server state.
func (t *serverStateProtocol) current() protocol.ServerState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.currentLocked()
}

// notifyIfChanged sends experimental/serverStateChanged when health or
// readiness changed since the last notification, if the client asked for it.
// The caller must hold t.mu.
func (t *serverStateProtocol) notifyIfChangedLocked(ctx context.Context, client protocol.Client) {
	state := t.currentLocked()
	if t.last != nil && t.last.Health == state.Health && t.last.Readiness == state.Readiness {
		return
	}
	t.last = &state
	if !t.clientWants {
		return
	}
	if err := protocol.NotifyServerStateChanged(ctx, client, &state); err != nil {
		event.Error(ctx, "sending experimental/serverStateChanged", err)
	}
}

// beginFolderLoad records that the initial load of a folder started.
func (t *serverStateProtocol) beginFolderLoad(ctx context.Context, client protocol.Client, uri protocol.DocumentURI) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.loading == 0 {
		// A new round of loads: the previous failures are superseded.
		t.failed = nil
	}
	t.loading++
	t.notifyIfChangedLocked(ctx, client)
}

// endFolderLoad records that the initial load of a folder finished, with
// errMsg describing the failure if it failed.
func (t *serverStateProtocol) endFolderLoad(ctx context.Context, client protocol.Client, uri protocol.DocumentURI, errMsg string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.loading > 0 {
		t.loading--
	}
	t.loaded = true
	if errMsg != "" {
		if t.failed == nil {
			t.failed = make(map[protocol.DocumentURI]string)
		}
		t.failed[uri] = errMsg
	}
	t.notifyIfChangedLocked(ctx, client)
}

// setCriticalError records the "Error loading workspace" status; an empty
// message clears it.
func (t *serverStateProtocol) setCriticalError(ctx context.Context, client protocol.Client, errMsg string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.critical = errMsg
	t.notifyIfChangedLocked(ctx, client)
}

// ServerState answers experimental/serverState.
func (s *server) ServerState(ctx context.Context) (any, error) {
	return s.serverState.current(), nil
}
