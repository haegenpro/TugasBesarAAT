package main

import (
	"testing"
)

func TestDecodeHazardListRejectsRenamedField(t *testing.T) {
	_, err := decodeHazardList([]byte(`{"data":[{"hazard_code":"id","source":"PVMBG","hazard_type":"VOLCANIC","severity":"SIAGA","area_name":"Merapi"}],"count":1,"as_of":"2026-09-30T00:00:00Z","source_status":{}}`))
	if err == nil {
		t.Fatal("renamed hazard_id was accepted")
	}
}

func TestDecodeHazardListAcceptsCurrentContract(t *testing.T) {
	list, err := decodeHazardList([]byte(`{"data":[{"hazard_id":"id","source":"PVMBG","hazard_type":"VOLCANIC","severity":"SIAGA","area_name":"Merapi"}],"count":1,"as_of":"2026-09-30T00:00:00Z","source_status":{}}`))
	if err != nil || len(list.Data) != 1 || list.Data[0].HazardID != "id" {
		t.Fatalf("current list contract rejected: list=%+v err=%v", list, err)
	}
}
