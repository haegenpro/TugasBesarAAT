package main

func loadConfig() (string, error) {
	v, err := requiredEnv("BNPB_API_URL", "MEDIA_CLIENT_ID", "FIELD_CLIENT_ID", "INTERNAL_CLIENT_ID")
	if err != nil {
		return "", err
	}
	if err = envURL(v, "BNPB_API_URL", "http", "https"); err != nil {
		return "", err
	}
	return v["BNPB_API_URL"], nil
}
