"""Regression guards for scheduler job trigger configuration.

The influencer job starved for ~25h on 2026-07-20 because IntervalTrigger
restarts its countdown on every app boot, and repeated Railway redeploys kept
resetting it. Low-frequency jobs must use CronTrigger (wall-clock anchored,
immune to restarts).
"""

from apscheduler.triggers.cron import CronTrigger

from pipeline.scheduler import scheduler


def test_influencer_uses_cron_trigger():
    job = scheduler.get_job("influencer")
    assert job is not None
    assert isinstance(job.trigger, CronTrigger)


def test_influencer_fires_every_six_hours_at_minute_20():
    trigger = scheduler.get_job("influencer").trigger
    fields = {f.name: str(f) for f in trigger.fields}
    assert fields["hour"] == "0,6,12,18"
    assert fields["minute"] == "20"


def test_all_expected_jobs_registered():
    expected = {
        "market", "market_eod", "narrative", "influencer", "macro_daily",
        "macro_intraday", "short_volume", "scoring_tick", "retention",
        "options", "demo_key_cleanup",
    }
    assert {job.id for job in scheduler.get_jobs()} == expected


def test_demo_key_cleanup_uses_cron_trigger():
    job = scheduler.get_job("demo_key_cleanup")
    assert job is not None
    assert isinstance(job.trigger, CronTrigger)
    fields = {f.name: str(f) for f in job.trigger.fields}
    assert fields["minute"] == "50"
