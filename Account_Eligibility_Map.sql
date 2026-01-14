
/* =========================================================
ACCOUNT ELIGIBILITY & ACTION TYPE MATRIX
========================================================= */

| Scenario | Main Exists? | Main Status | Sub Exists? | Sub Status | Eligibility | Action Type |
|----------|--------------|------------|------------|------------|------------|------------|
| 1 | Yes | Active | No | N/A | ELIGIBLE_CLOSE | CLOSE_MAIN |
| 2 | Yes | Active | Yes | All Closed | ELIGIBLE_CLOSE | CLOSE_MAIN |
| 3 | Yes | Active | Yes | Some Active | ELIGIBLE_CLOSE | CLOSE_SUB_AND_MAIN |
| 4 | Yes | Closed | Yes | Some Active | DO_NOT_CLOSE | INVESTIGATE |
| 5 | No | N/A | Yes | Some Active | DO_NOT_CLOSE | INVESTIGATE |
| 6 | Yes | Closed | Yes | All Closed | SKIP | NA |
| 7 | Yes | Closed | No | N/A | SKIP | NA |
| 8 | No | N/A | No | N/A | SKIP | NA |
| 9 | Not in system | N/A | N/A | N/A | SKIP | NA |
