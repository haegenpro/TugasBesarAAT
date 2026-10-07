package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/term"
)

type session struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	Role         string `json:"role"`
	ClientID     string `json:"client_id"`
	Scope        string `json:"scope"`
}

type app struct {
	input          *bufio.Reader
	client         *http.Client
	baseURL        string
	session        session
	previousAccess string
}

func main() {
	baseURL, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}

	a := &app{input: bufio.NewReader(os.Stdin), client: &http.Client{Timeout: 8 * time.Second}, baseURL: strings.TrimRight(baseURL, "/")}
	a.run()
}

func (a *app) run() {
	for {
		a.clear()
		fmt.Println("BNPB | Client Downstream")
		fmt.Println("API:", a.baseURL)
		if a.session.AccessToken == "" {
			fmt.Println("\n1. Login Media\n2. Login Tim Lapangan\n3. Login Internal\n0. Keluar")
			switch a.prompt("Pilih") {
			case "1":
				a.login("MEDIA")
			case "2":
				a.login("FIELD")
			case "3":
				a.login("INTERNAL")
			case "0":
				return
			}
			continue
		}
		fmt.Printf("Login: %s (%s)\n", a.session.ClientID, a.session.Role)
		fmt.Println("\n1. Daftar hazard\n2. Daftar dengan filter\n3. Detail hazard\n4. Minta data raw\n5. Uji token lama\n6. Logout\n7. Laporan awal\n8. Detail laporan awal\n9. Riwayat laporan awal\n0. Keluar")
		switch a.prompt("Pilih") {
		case "1":
			a.list("")
		case "2":
			a.filteredList()
		case "3":
			a.detail()
		case "4":
			a.requestRaw()
		case "5":
			a.testOldToken()
		case "6":
			a.logout()
		case "7":
			a.notices(false)
		case "8":
			a.noticeDetail()
		case "9":
			a.notices(true)
		case "0":
			return
		}
	}
}

func (a *app) clear() {
	fmt.Print("\033[2J\033[H")
}

func (a *app) prompt(label string) string {
	fmt.Print(label, ": ")
	line, _ := a.input.ReadString('\n')
	return strings.TrimSpace(line)
}

func (a *app) secret() string {
	fmt.Print("Secret: ")
	if term.IsTerminal(int(os.Stdin.Fd())) {
		value, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err == nil {
			return string(value)
		}
	}
	line, _ := a.input.ReadString('\n')
	return strings.TrimSpace(line)
}

func (a *app) pause() {
	a.prompt("Tekan Enter untuk lanjut")
}

func (a *app) request(method, path string, payload any, access string) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, a.baseURL+path, body)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if access != "" {
		req.Header.Set("Authorization", "Bearer "+access)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, data, err
}

func (a *app) protected(method, path string) (int, []byte, error) {
	status, data, err := a.request(method, path, nil, a.session.AccessToken)
	if err != nil || status != http.StatusUnauthorized || a.session.RefreshToken == "" {
		return status, data, err
	}
	oldAccess := a.session.AccessToken
	refreshStatus, refreshData, refreshErr := a.request(http.MethodPost, "/auth/refresh", map[string]string{"refresh_token": a.session.RefreshToken}, "")
	if refreshErr != nil {
		return status, data, fmt.Errorf("auth service belum tersedia: %w", refreshErr)
	}
	if refreshStatus == http.StatusUnauthorized {
		a.session = session{}
		return status, data, fmt.Errorf("sesi habis; silakan login ulang")
	}
	if refreshStatus != http.StatusOK {
		return status, data, fmt.Errorf("auth service belum tersedia: HTTP %d", refreshStatus)
	}
	var next session
	if err := json.Unmarshal(refreshData, &next); err != nil {
		return status, data, err
	}
	a.session = next
	a.previousAccess = oldAccess
	fmt.Println("Access token kedaluwarsa; token baru diterbitkan tanpa login ulang.")
	return a.request(method, path, nil, a.session.AccessToken)
}

func (a *app) login(prefix string) {
	clientID := os.Getenv(prefix + "_CLIENT_ID")
	if clientID == "" {
		fmt.Println(prefix + "_CLIENT_ID is required")
		a.pause()
		return
	} else {
		fmt.Println("Client ID:", clientID)
	}
	secret := a.secret()
	status, data, err := a.request(http.MethodPost, "/auth/login", map[string]string{"client_id": clientID, "client_secret": secret}, "")
	if err != nil {
		fmt.Println("Gagal terhubung:", err)
		a.pause()
		return
	}
	if status != http.StatusOK {
		fmt.Printf("Login ditolak: HTTP %d\n", status)
		a.pause()
		return
	}
	if err := json.Unmarshal(data, &a.session); err != nil {
		fmt.Println("Respons login tidak valid:", err)
		a.pause()
		return
	}
	a.previousAccess = ""
	fmt.Printf("Login berhasil sebagai %s. TTL access token: %d detik.\n", a.session.Role, a.session.ExpiresIn)
	a.pause()
}

func (a *app) list(view string) {
	query := url.Values{}
	if view != "" {
		query.Set("view", view)
	}
	a.showList(query)
}

func (a *app) requestRaw() {
	status, data, err := a.protected(http.MethodGet, "/hazards?view=raw")
	if err != nil {
		fmt.Println("Gagal:", err)
		a.pause()
		return
	}
	fmt.Printf("HTTP %d\n", status)
	if status == http.StatusOK {
		var result struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := requireResponseFields(data, "data"); err != nil {
			fmt.Println("Respons tidak valid:", err)
		} else if err := json.Unmarshal(data, &result); err != nil {
			fmt.Println("Respons tidak valid:", err)
		} else if len(result.Data) > 0 {
			fmt.Printf("Data raw: %d hazard\n", len(result.Data))
			for i, item := range result.Data {
				fmt.Printf("\n[%d/%d]\n%s\n", i+1, len(result.Data), pretty(item))
			}
		} else {
			fmt.Println("Data kosong")
		}
	} else {
		fmt.Println(pretty(data))
	}
	a.pause()
}

func (a *app) filteredList() {
	query := url.Values{}
	for _, item := range []struct{ prompt, key string }{
		{"Jenis SEISMIC/VOLCANIC (opsional)", "hazard_type"},
		{"Severity NORMAL/WASPADA/SIAGA/AWAS (opsional)", "severity"},
		{"Nama area (opsional)", "area_name"},
		{"Sejak waktu ISO 8601 (opsional)", "since"},
	} {
		if value := a.prompt(item.prompt); value != "" {
			query.Set(item.key, value)
		}
	}
	a.showList(query)
}

func (a *app) showList(query url.Values) {
	path := "/hazards"
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	status, data, err := a.protected(http.MethodGet, path)
	if err != nil {
		fmt.Println("Gagal:", err)
		a.pause()
		return
	}
	if status != http.StatusOK {
		fmt.Printf("HTTP %d: %s\n", status, string(data))
		a.pause()
		return
	}
	result, err := decodeHazardList(data)
	if err != nil {
		fmt.Println("Respons tidak valid:", err)
		a.pause()
		return
	}
	fmt.Printf("\n%d hazard | as_of %s\n", result.Count, result.AsOf)
	fmt.Printf("%-36s  %-6s  %-8s  %-7s  %s\n", "Hazard ID", "Sumber", "Jenis", "Level", "Area")
	for _, item := range result.Data {
		fmt.Printf("%-36s  %-6s  %-8s  %-7s  %s\n", item.HazardID, item.Source, item.HazardType, item.Severity, item.AreaName+" | waktu: "+item.OccurredAt)
	}
	fmt.Println("Status sumber:", pretty(result.SourceStatus))
	if len(result.Metadata) > 0 {
		fmt.Println("Kebaruan / assessment:", pretty(result.Metadata))
	}
	a.pause()
}

func (a *app) detail() {
	id := a.prompt("Hazard ID")
	status, data, err := a.protected(http.MethodGet, "/hazards/"+url.PathEscape(id))
	if err != nil {
		fmt.Println("Gagal:", err)
	} else {
		fmt.Printf("HTTP %d\n%s\n", status, pretty(data))
	}
	a.pause()
}

func (a *app) testOldToken() {
	if a.previousAccess == "" {
		fmt.Println("Belum ada token lama. Tunggu TTL habis, lalu buka daftar hazard untuk memicu refresh.")
		a.pause()
		return
	}
	status, _, err := a.request(http.MethodGet, "/hazards", nil, a.previousAccess)
	if err != nil {
		fmt.Println("Gagal:", err)
	} else {
		fmt.Printf("Permintaan dengan token lama: HTTP %d (target: 401)\n", status)
	}
	a.pause()
}

func (a *app) logout() {
	if a.session.RefreshToken != "" {
		status, _, err := a.request(http.MethodPost, "/auth/logout", map[string]string{"refresh_token": a.session.RefreshToken}, "")
		if err != nil || status != http.StatusNoContent {
			fmt.Printf("Logout server gagal: HTTP %d, %v\n", status, err)
			a.pause()
			return
		}
	}
	a.session = session{}
	a.previousAccess = ""
}

func pretty(data []byte) string {
	var output bytes.Buffer
	if json.Indent(&output, data, "", "  ") == nil {
		return output.String()
	}
	return string(data)
}

func (a *app) notices(history bool) {
	path := "/notices?view=summary"
	if history {
		path += "&state=all"
	}
	status, data, err := a.protected(http.MethodGet, path)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("HTTP", status)
		fmt.Println(pretty(data))
	}
	a.pause()
}
func (a *app) noticeDetail() {
	id := a.prompt("Notice ID")
	status, data, err := a.protected(http.MethodGet, "/notices/"+url.PathEscape(id))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("HTTP", status)
		fmt.Println(pretty(data))
	}
	if status == 200 {
		var response struct {
			Data struct {
				HazardID string `json:"hazard_id"`
			}
		}
		if json.Unmarshal(data, &response) == nil && response.Data.HazardID != "" && a.prompt("Buka hazard terkait? (y/n)") == "y" {
			status, data, err = a.protected(http.MethodGet, "/hazards/"+url.PathEscape(response.Data.HazardID))
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("HTTP", status)
				fmt.Println(pretty(data))
			}
		}
	}
	a.pause()
}
