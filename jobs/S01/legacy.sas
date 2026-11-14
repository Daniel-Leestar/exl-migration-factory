/* S01: first (smallest) claim of each policy, using DATA step FIRST. processing. */
proc sort data=work.claims out=work.claims_sorted;
    by IDpol ClaimAmount;
run;

data work.first_claim;
    set work.claims_sorted;
    by IDpol;
    if first.IDpol;
    keep IDpol ClaimAmount;
run;
