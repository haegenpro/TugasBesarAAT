package main

import "testing"

func TestRecoveryRequiresOriginalSurvivingQuorumAndCurrentReplacement(t *testing.T) {
	expected := map[string]bool{"a": true, "b": true, "c": true}
	state := streamState{Name: "BNPB_CRITICAL", Replicas: 3, Leader: "a", Peers: []peer{{Name: "b", Current: true}, {Name: "c", Offline: true}}}
	if err := validate(state, "c", expected, false); err != nil {
		t.Fatal(err)
	}
	if err := validate(state, "c", expected, true); err == nil {
		t.Fatal("offline replacement accepted")
	}
	state.Peers[0].Current = false
	if err := validate(state, "c", expected, false); err == nil {
		t.Fatal("single surviving copy accepted")
	}
	state.Peers = []peer{{Name: "b", Current: true}, {Name: "c", Current: true}}
	if err := validate(state, "c", expected, true); err != nil {
		t.Fatal(err)
	}
	state.Peers[1].Name = "foreign"
	if err := validate(state, "c", expected, true); err == nil {
		t.Fatal("foreign replacement accepted")
	}
}
