/* S03: loss summary per region, using PROC SQL then PROC MEANS. */
proc sql;
    create table work.policy_claims as
    select p.Region, p.IDpol, c.ClaimAmount
    from work.policy as p
    left join work.claims as c
      on p.IDpol = c.IDpol;
quit;

proc means data=work.policy_claims noprint nway;
    class Region;
    var ClaimAmount;
    output out=work.region_loss (drop=_type_ _freq_)
        n=ClaimCount sum=TotalLoss mean=AvgLoss;
run;
