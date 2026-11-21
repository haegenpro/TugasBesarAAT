package main

func validSource(v string) bool {
	return v == "BMKG" || v == "PVMBG" || v == "SATELIT" || v == "SENSOR_PANTAI"
}
func validHazardType(v string) bool {
	return v == "SEISMIC" || v == "VOLCANIC" || v == "WILDFIRE" || v == "COASTAL"
}
func validSeverity(v string) bool {
	return v == "NORMAL" || v == "WASPADA" || v == "SIAGA" || v == "AWAS"
}
