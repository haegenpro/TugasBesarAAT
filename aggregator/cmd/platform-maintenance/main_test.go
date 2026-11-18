package main

import (
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func TestAdminDSNPreservesSecretWithoutShellExpansion(t *testing.T) {
	password := "special:' p@ss/word?&$(literal)"
	u, err := url.Parse(adminDSN("storage-primary.namespace.svc.cluster.local", "hazards", password))
	require.NoError(t, err)
	actual, ok := u.User.Password()
	require.True(t, ok)
	require.Equal(t, password, actual)
	require.Equal(t, "postgres", u.User.Username())
	require.Equal(t, "5432", u.Port())
	for _, action := range []string{"canonical", "ingestion", "cutover"} {
		_, _, err = command(action, "namespace")
		require.NoError(t, err)
	}
	_, _, err = command("cutover", "")
	require.Error(t, err)
	_, _, err = command("arbitrary-command", "namespace")
	require.Error(t, err)
}
