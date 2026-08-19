InsightFlow — Video Walkthrough Script
========================================
Target length: 8-12 minutes. Sections marked [SHOW: ...] indicate what to
have on screen. Delivery notes in [brackets, italics implied].

------------------------------------------------------------
1. OPENING (30-45 sec)
------------------------------------------------------------
[SHOW: Architecture diagram or the S3 bucket root, showing raw/processed/marts/]

"This is InsightFlow — an AWS-native pipeline that pulls data from three
systems DE Academy actually uses: Close for CRM leads, Calendly for
bookings and ad spend, and Wistia for video engagement. The goal is one
place to answer: is marketing spend actually producing leads and booked
calls?

Everything here is real, deployed infrastructure — not a local demo. I'll
walk through the architecture end to end, then spend a few minutes on
some of the more interesting problems I ran into with real production
data, because a couple of them changed how the pipeline actually works."

------------------------------------------------------------
2. INGESTION LAYER (90 sec)
------------------------------------------------------------
[SHOW: Lambda console, list of 5 functions]

"Three sources, two ingestion patterns. Close and Calendly both push data
to me via webhooks — API Gateway plus a Lambda that validates the request
and writes it straight to S3, untouched. Wistia doesn't push anything, so
I pull it on a schedule instead — EventBridge triggers a Lambda every 6
hours.

[SHOW: CRM webhook Lambda -> SQS delay queue in console]

The CRM flow has one wrinkle worth mentioning: when a lead is created in
Close, the owner isn't assigned yet — that happens on Close's side,
asynchronously. So the webhook Lambda writes the raw event, then drops a
message on an SQS queue with a 10-minute delay. A second Lambda picks it
up after that delay, looks up the now-assigned owner from a lookup
bucket, merges the two, and posts to Slack.

[SHOW: CloudWatch Logs for crm_consumer_handler, a real successful run]

This is also where I do idempotency — if the same webhook event gets
redelivered, which SQS can do, the consumer checks whether that lead's
already been processed and just no-ops. And fault tolerance: three failed
attempts and a message drops into a dead-letter queue with a CloudWatch
alarm, instead of retrying forever."

------------------------------------------------------------
3. TRANSFORM LAYER (60-75 sec)
------------------------------------------------------------
[SHOW: Glue console, the two Glue jobs]

"Raw data is untouched JSON. Two Glue Python Shell jobs clean it up —
one for Calendly, one for Wistia. CRM doesn't get a separate Glue job;
that enrichment happens right in the consumer Lambda, since it's a
real-time, single-record operation, not a batch job.

[SHOW: S3 processed/ folder, six subfolders]

Calendly splits into two tables — bookings and spend — because they're
genuinely different things that get joined later, not now. Wistia splits
into three — metadata, engagement, and events — because those are three
different grains of data: one row per video, one row per engagement
snapshot, one row per individual viewer action. Cramming those into one
table would've meant either sparse columns or losing the event-level
detail entirely."

------------------------------------------------------------
4. CATALOG + MARTS (60 sec)
------------------------------------------------------------
[SHOW: Glue Data Catalog, insightflow database, 6 tables]

"A Glue Crawler registers all six processed tables into a Glue Catalog so
Athena can query them like a database, without an actual database server
to manage.

[SHOW: Athena console, one of the marts tables]

On top of that sits the marts layer — seven tables, built by a Lambda
that runs CTAS queries: daily bookings by channel, cost per booking, a
booking heat map by day and hour, meeting load per employee, and a
cross-source correlation between Calendly bookings and CRM leads by
date and channel."

------------------------------------------------------------
5. DASHBOARD (45 sec)
------------------------------------------------------------
[SHOW: QuickSight dashboard, scroll through the visuals]

"QuickSight sits on top of the marts, querying Athena directly rather
than importing a static snapshot — so it's always showing current data,
not a stale copy. One visual per mart: bookings by channel, the
booking-volume heat map, meeting load by employee, and this — channel
attribution — which is where most of the interesting debugging happened."

------------------------------------------------------------
6. THE INVESTIGATION — pick 2-3 of these based on time available
------------------------------------------------------------

--- STORY A: The stale channel mapping (strongest story, use this one) ---
[SHOW: Section 15, Finding 1 in the design doc, or the before/after Athena query]

"An SME flagged something real: cost-per-booking was showing zero, even
though $100k+ had actually been spent on ads. That sent me back into the
data.

The original approach matched on a UTM tracking field Calendly sends —
except real data showed that field was populated maybe 1% of the time,
and when it WAS populated, it was full of visitor IP addresses, not
channel names. Not a bug on my end — that's an ad-platform tracking
configuration issue upstream.

But re-reading the requirements doc more carefully, there was actually a
better field the whole time: Calendly assigns a fixed event_type URL to
every booking page. The doc even gave three reference IDs for Facebook,
YouTube, and TikTok. So I checked those three IDs against real, current
booking data.

[SHOW: the LEFT JOIN query result — 1, 0, 6]

YouTube matched. Facebook matched exactly one historical booking.
TikTok matched zero. Only seven bookings out of six hundred seventy-three
matched the documented mapping at all — about one percent.

Most likely explanation: Calendly assigns a new ID every time a booking
page gets rebuilt, and the Facebook and TikTok pages had been recreated
at some point after the reference doc was written, with nothing in the
data flagging that drift.

[SHOW: the real, current event_type IDs found in the data]

So I built a new mapping from the real, currently-active IDs instead,
verified it against the actual meeting names in the data, deployed it,
and reran the marts. Channel attribution went from one bucket —
'organic, unknown' — to real counts across all three channels."

--- STORY B: The spend duplication bug (second-strongest, shorter) ---
[SHOW: the duplicate-row Athena query results]

"Fixing the channel mapping surfaced a second problem — the booking
counts in one mart didn't match the same count in the raw table.
Facebook showed 146 bookings in one place, 82 in another.

Turned out the daily ad-spend file covers a rolling 30-day window, and my
transform job read every file on every run without deduplicating. So the
same day's spend number was getting counted multiple times — up to eight
times for some dates — which inflated every spend-based metric.

The fix was a deduplication step, plus a safety check: if two
'duplicate' rows ever actually disagree on the spend amount, that's not
harmless overlap, that's a real data revision, and I wanted that to throw
a warning instead of silently picking one. Redeployed, reran, and the
numbers lined up."

--- STORY C: T-1 lookup timing (shorter, use if time permits) ---
[SHOW: the 850/989 query result]

"One more real finding: 86% of CRM leads are missing an owner and email.
Traced it to timing — the lookup data I depend on is a full day behind,
but my pipeline only waits ten minutes before checking it once. Ten
minutes can't bridge a 24-hour gap except by luck, and since each lead
only gets looked up once, it never gets a second chance. This matches
what the SME confirmed the intended design actually is, so it's
documented as an accepted limitation, not something I patched around."

------------------------------------------------------------
7. CLOSING (20-30 sec)
------------------------------------------------------------
[SHOW: the design doc's table of contents, or the architecture diagram again]

"End to end: real webhooks and scheduled pulls, Glue transforms, a
cataloged lakehouse, Athena marts, and a live QuickSight dashboard — all
built on infrastructure as code, all tested, and all validated against
real production data rather than assumptions. The full design doc,
including every finding I just walked through, is in the repo."

[END]

------------------------------------------------------------
DELIVERY NOTES
------------------------------------------------------------
- If short on time, cut Section 3 (Transform) down to one sentence per
  source and cut Story C entirely. Story A is the one worth keeping no
  matter what — it's the clearest example of real, evidence-based
  debugging in the whole project.
- Have the design doc open in a second tab/window so you can jump to
  Section 15 directly if asked a follow-up question live.
- If recording screen + voice separately: capture the QuickSight
  dashboard scroll and the Athena query results FIRST, since those are
  the most likely to need a retake if a number looks off.
