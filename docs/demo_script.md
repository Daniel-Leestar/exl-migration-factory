# Demo video script (3 minutes)

What to click and what to say. The numbers assume a run with the local model on:
13 jobs, 7 Verified, 1 Tested, 5 Needs review. Check the Overview page before recording and
change the numbers in the script if yours differ.

## Before recording

1. Start Ollama, then from the project root:
   ```
   python -m engine run-all
   python -m engine export
   streamlit run ui/app.py
   ```
   `run-all` with the model takes about 15 minutes, mostly on J08 and J09.
2. Make sure no job is signed off yet (Overview shows "Signed off for Gold: 0").
3. Copy this query somewhere handy for step 6:
   ```sql
   SELECT p.Area, COUNT(*) AS Policies, AVG(p.DrivAge) AS AvgAge
   FROM dbo.policy AS p
   GROUP BY p.Area;
   ```
4. Rehearse step 4 once (sign off, deploy, then withdraw the sign-off) so the Gold deployment is not running for the first time on camera.

## Script

| Time | On screen | Say |
|---|---|---|
| 0:00-0:20 | **Overview** page. Point at the five numbers, then the two progress bars. | "This is our migration factory for Challenge 3. We took 13 legacy jobs, ten in T-SQL and three in SAS, and moved them to PySpark. Seven are Verified, one is Tested, five still need review. Nothing has been signed off yet, so nothing can reach the Gold layer." |
| 0:20-0:55 | **Job detail**, choose **J04**. Show the Code tab, scroll to "Why the PySpark looks this way", then open **Reconciliation**. | "J04 works out the share of policies with a claim per region. In SQL Server, an integer divided by an integer drops the fraction. Spark keeps it. If you translate this line by line, the job still runs and every number is slightly wrong. Our rules know this and add the cast. Then we check: 21 rows from the legacy system, 21 from Spark, and they match on real sample data, on awkward test data we generate, and on empty tables." |
| 0:55-1:25 | Choose **J10**. Open **AI audit**, expand the first call. | "J10 uses UNION ALL, which our rules do not cover, so it went to a small model running on this laptop. Here is the exact prompt and the answer. The model only sees the code, the table structure and made-up rows. Its code went through the same check as the rule-based code, and it matched, so it is Tested. Not Verified, because a person should read AI-written code more carefully." |
| 1:25-2:00 | Back to **J04**, tab **Review & actions**. Type your name, tick the box, click **Sign off for Gold**. Go to **Overview**, click **Run the Gold deployment gate**, open the output. | "A passing check is not an approval. Someone has to put their name on it. I sign off J04. The sign-off is tied to this exact version of the code, so if the code changes, it lapses. Now I run the Gold gate. J04 goes through. Every other job is blocked, either because nobody signed it or because it failed the check." |
| 2:00-2:25 | **Job detail**, choose **J09**. Show the Next step line and the Reconciliation tab. | "This is what a failure looks like. J09 is a stored procedure. The model tried three repairs and 17 rows still differ, so it stays in Needs review and the page does not offer a sign-off. The three SAS jobs are in the same state for a different reason: we had no SAS runtime, so there is no legacy output to compare against." |
| 2:25-2:45 | **Try your own code**. Paste the query, click **Check this code**. | "You can also paste a query you have never run through the factory. It tells you straight away whether the rules can translate it, shows the PySpark, and lists the SQL Server behaviour it had to copy. Here that is AVG on whole numbers, and GROUP BY ignoring upper and lower case." |
| 2:45-3:00 | **Analytics**. Then the green CI run on GitHub. | "Eight of 13 jobs were migrated without anyone writing PySpark by hand, and CI repeats the check on every push. What we deliver is the code plus the evidence that lets someone trust it. Thank you." |

About 430 words, which fits three minutes at a normal pace. If you run over, cut step 6.
