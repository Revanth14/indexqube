package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type githubRelease struct {
	TagName string `json:"tag_name"`
}

func checkForUpdate() {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("https://api.github.com/repos/Revanth14/indexqube/releases/latest")
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return
	}

	if release.TagName == "" || release.TagName == version {
		return
	}

	fmt.Fprintf(os.Stderr,
		"\niq: new version available %s → %s\n"+
			"    Download the latest release from: https://github.com/Revanth14/indexqube/releases/latest\n\n",
		version, release.TagName,
	)
}
