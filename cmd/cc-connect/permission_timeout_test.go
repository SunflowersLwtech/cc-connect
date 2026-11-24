package main

import (
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/config"
)

func TestPermissionTimeout_ProjectOverridesTopLevel(t *testing.T) {
	n := func(v int) *int { return &v }
	cases := []struct {
		name      string
		top, proj *int
		want      time.Duration
	}{
		{"neither set waits forever", nil, nil, 0},
		{"top level", n(10), nil, 10 * time.Minute},
		{"project overrides", n(10), n(3), 3 * time.Minute},
		{"project 0 turns it off", n(10), n(0), 0},
		{"negative is off", n(-5), nil, 0},
	}
	for _, c := range cases {
		cfg := &config.Config{PermissionTimeoutMins: c.top}
		proj := &config.ProjectConfig{PermissionTimeoutMins: c.proj}
		if got := permissionTimeout(cfg, proj); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	if got := permissionTimeout(&config.Config{PermissionTimeoutMins: n(7)}, nil); got != 7*time.Minute {
		t.Errorf("nil project: %v", got)
	}
}
