package main

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayCLIAndBlockedFixture(t *testing.T) {
	root := filepath.Join("..", "..", "..", "docs", "fixtures", "wildfire-m5")
	args := []string{"replay", "--config", filepath.Join(root, "config.json"), "--input", filepath.Join(root, "bridge.jsonl")}
	var first, second, stderr bytes.Buffer
	require.NoError(t, run(args, &first, &stderr))
	require.NoError(t, run(args, &second, &stderr))
	require.Equal(t, first.String(), second.String())
	require.Equal(t, 6, strings.Count(first.String(), "\n"))
	require.NotContains(t, first.String(), `"hazard_id"`)
	require.NotContains(t, first.String(), `"ingested_at"`)
	args[len(args)-1] = filepath.Join(root, "mixed-merge-split.jsonl")
	first.Reset()
	require.Error(t, run(args, &first, &stderr))
	require.Contains(t, first.String(), "ambiguous_merge_split")
	input := filepath.Join(t.TempDir(), "invalid.jsonl")
	require.NoError(t, os.WriteFile(input, []byte("{\"version\":1}\n"), 0600))
	args[len(args)-1] = input
	first.Reset()
	require.Error(t, run(args, &first, &stderr))
	require.Contains(t, first.String(), "input_base64")
	require.Contains(t, first.String(), `"rejected":true`)
	require.Error(t, run([]string{"replay"}, &first, &stderr))
}
