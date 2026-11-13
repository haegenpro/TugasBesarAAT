-- Live polling, history replay and deferred mapping share one source owner.
-- These scopes are separate from canonical delivery; epochs survive handoff.
INSERT INTO canonical_ownership(scope,source) VALUES
 ('acquisition:BMKG','BMKG'), ('acquisition:PVMBG','PVMBG');
