# Remove RETURN-STRICT from 8.8-rse

## Why

[MOD-19180](https://redislabs.atlassian.net/browse/MOD-19180) requests removal of the RETURN-STRICT timeout policy from the 8.8-rse release branch. Its main-thread partial-result callbacks require synchronization with background result production.

## What Changes

Only RETURN and FAIL remain accepted by ON_TIMEOUT and search-on-timeout. Configurations using RETURN-STRICT must select one of these policies before upgrading; the removed value is rejected rather than silently mapped to another policy. Defaults and the behavior of the remaining policies do not change.
