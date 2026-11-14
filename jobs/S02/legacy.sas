/* S02: running total of claim amounts within each policy, using RETAIN. */
proc sort data=work.claims out=work.claims_sorted;
    by IDpol ClaimAmount;
run;

data work.claims_running;
    set work.claims_sorted;
    by IDpol;
    retain RunningTotal 0;
    if first.IDpol then RunningTotal = 0;
    RunningTotal = sum(RunningTotal, ClaimAmount);
run;
