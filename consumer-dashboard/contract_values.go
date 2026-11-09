package main

func validSource(v string) bool     { return v == "BMKG" || v == "PVMBG" }
func validHazardType(v string) bool { return v == "SEISMIC" || v == "VOLCANIC" }
func validSeverity(v string) bool {
	return v == "NORMAL" || v == "WASPADA" || v == "SIAGA" || v == "AWAS"
}
