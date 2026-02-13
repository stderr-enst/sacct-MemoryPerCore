#! /usr/bin/env python

import datetime
from dateutil import rrule
from io import StringIO,BytesIO
import logging
import matplotlib.pyplot as plt
import numpy
import os
import polars as pl
import subprocess


def getWeeksList(startdate="2024-01-01", enddate="2024-12-31"):
    """
    Prepare a list of all start- and end-days of each week in the given range.
    Includes startdate and enddate
    """
    try:
        day_start = datetime.datetime.fromisoformat(startdate)
        day_end = datetime.datetime.fromisoformat(enddate)
        daystoprocess = (day_end - day_start).days + 1
        secondstoprocess = (day_end - day_start).seconds
    except ValueError:
        logging.warning(f"'{startdate}' and '{enddate}' are not a valid input")
        daystoprocess = 0

    weekintervals = []

    if daystoprocess <= 0:
        # Not a valid interval
        logging.info(f"Interval is empty")
        pass
    elif secondstoprocess > 0 and secondstoprocess < 86400:
        # Don't assume running for whole days
        weekintervals += [(day_start, day_end)]
    elif daystoprocess < 7:
        # If interval is shorter than a week
        # We have to add 23h 59m 59s, such that we always go through the whole day
        weekintervals += [(day_start, day_end + datetime.timedelta(hours=23, minutes=59, seconds=59))]
    else:
        # Handle first week not starting on Monday
        days_until_sunday = 7 - day_start.isoweekday()
        weekend = day_start + datetime.timedelta(days=days_until_sunday)
        # We have to add 23h 59m 59s, such that we always go through the whole day
        weekintervals += [(day_start, weekend + datetime.timedelta(hours=23, minutes=59, seconds=59))]

        # Handle all following weeks until day_end
        nextmonday = weekend + datetime.timedelta(days=1)

        for weekstart in rrule.rrule(rrule.WEEKLY, dtstart=nextmonday, until=day_end):
            weekend = weekstart + datetime.timedelta(days=6)
            if weekend > day_end:
                # Don't go past the last date
                weekend = day_end
            # We have to add 23h 59m 59s, such that we always go through the whole day
            weekintervals += [(weekstart, weekend + datetime.timedelta(hours=23, minutes=59, seconds=59))]

    return weekintervals


def getsacctData(startdate="2024-01-01", enddate="2024-01-06"):
    """
    Read sacct output into dataframe.
    The sacct command is similar to:

    $ sacct --format=JobIDraw%20,MaxRSS%20,AllocCPUS --parsable2 --state=completed -S 2026-01-10 -E 2026-02-01
    JobIDRaw|MaxRSS|AllocCPUS
    17031184||1
    17031184.extern|208K|1
    17031184.0|43992K|1
    17031931||4
    17031931.batch|46908K|1
    17031931.extern|212K|4
    17031931.0|40052K|4
    17032495||4
    17032495.batch|1299472K|1
    17032495.extern|212K|4
    17032495.0|1381848K|4
    17032701||4
    17032701.batch|59460K|1
    17032701.extern|208K|4
    17032701.0|4972K|4
    17032701.1|5220K|4
    17032701.2|4972K|4
    17032701.3|36152K|4
    17032701.4|13024K|4
    17032701.5|16224K|4

    So MaxRSS results are attributed to individual job steps!
    """

    logging.debug(f"Trying to call sacct from {startdate} to {enddate}")
    # Abort if date-range is too large! Protecting SlurmDB from out-of-memory
    day_start = datetime.datetime.fromisoformat(startdate)
    day_end = datetime.datetime.fromisoformat(enddate)
    daystoprocess = (day_end - day_start).days + 1
    if daystoprocess > 7:
        logging.error("Data range in sacct request too large!")
        return
    elif daystoprocess <= 0:
        logging.warning("No data range specified")
        return

    sacct_command = [
            "sacct",
            "--allusers",
            "--format=JobID%20,Start%20,User%20,Account%20,CPUTimeRAW,MaxRSS%20,AllocCPUS%20",
            "--parsable2",
            "--starttime",
            startdate,
            "--endtime",
            enddate
            ]

    sacct_call = subprocess.run(sacct_command, capture_output=True)

    logging.debug(sacct_call.args)
    logging.debug(sacct_call.returncode)
    logging.debug(sacct_call.stdout)
    logging.debug(sacct_call.stderr)

    return sacct_call.stdout


def readDataFrame(startdate="2024-01-01", enddate="2024-12-31", forceRead=False):
    weekintervals = getWeeksList(startdate, enddate)
    weekdfs = []

    logging.info("Start reading SlurmDB into DataFrames")
    for weekstart, weekend in weekintervals:
        logging.info(f"\tPreparing df from {weekstart} to {weekend}")
        year, week, day = weekstart.isocalendar()
        cachedir = "./cache"
        cachefile = cachedir+f"/{year}-{week}.parquet"
        if not os.path.isdir(cachedir):
            os.makedirs(cachedir)
        if os.path.isdir(cachedir) and os.path.exists(cachefile) and not forceRead:
            logging.info(f"\tFound cached dataframe, reading it from {cachefile}")
            weekdf = pl.read_parquet(cachefile)
        else:
            sacctoutput = getsacctData(weekstart.isoformat(), weekend.isoformat())
            logging.info(f"\tStart building df for week")
            # Use BytesIO to act as if the csv has been read from a file
            weekdf = pl.read_csv(BytesIO(sacctoutput), separator='|', schema_overrides={
                "JobID" : str,
                "Start" : pl.Datetime,
                "User" : str,
                "Account" : str,
                "CPUTimeRAW" : pl.Int64,
                "MaxRSS" : str,
                "AllocCPUS" : pl.Int64,
                }, null_values=["None","Unknown"], try_parse_dates=True)

            # Calculate MaxRSS to Bytes
            multiplier = (
                pl.when(pl.col("MaxRSS").str.ends_with("K")).then(1_000)
                .when(pl.col("MaxRSS").str.ends_with("M")).then(1_000_000)
                .when(pl.col("MaxRSS").str.ends_with("G")).then(1_000_000_000)
                .otherwise(1)
            )
            weekdf = weekdf.with_columns(
                memorybytes=(
                    pl.col("MaxRSS")
                    .str.replace(r'[KMG]$', '') # drop K, M or G at end.
                    .cast(pl.Float64)
                    .mul(multiplier)            # suffix is dropped only after command, so still works here
                ).cast(pl.Int64)
            )

            # CPUTime in seconds to core-h
            weekdf = weekdf.with_columns(
                CoreH=(
                    pl.col("CPUTimeRAW") / 60.0 / 60.0
                )
            )

            # Calculate GB / Core
            weekdf = weekdf.with_columns(
                MemPerCore=(
                    pl.col("memorybytes") / pl.col("AllocCPUS") * 1.0e-9
                )
            )

            # Reorder columns
            weekdf = weekdf.select(["JobID", "Start", "Account", "User", "CPUTimeRAW", "CoreH", "MaxRSS", "memorybytes", "AllocCPUS", "MemPerCore"])

            # Remove entries without MaxRSS, e.g. reported job allocation
            weekdf = weekdf.filter(pl.col("MemPerCore").is_not_null())

            weekdf.write_parquet(cachefile)
        weekdfs += [weekdf]

    logging.info("Merging all DataFrames")
    alldfs = pl.concat(weekdfs)

    if alldfs["JobID"].is_duplicated().any():
        logging.warning("Found duplicate jobs, only keeping the first")
        alldfs = alldfs.unique(subset="JobID", keep="first")

    # calculate time interval
    day_start = datetime.datetime.fromisoformat(startdate)
    day_end = datetime.datetime.fromisoformat(enddate)
    period = (day_end - day_start)

    alldfs = alldfs.sort("MemPerCore", descending=False)

    return alldfs, period

def plot_cumsum(df, filename):
    fig, ax = plt.subplots()
    df = df.with_columns(CoreH_cumsum=pl.col("CoreH").cum_sum())

    # Optionally normalize to read of percentage:
    # df = df.with_columns(CoreH_cumsum=pl.col("CoreH").cum_sum() / pl.col("CoreH").sum())

    ax.plot(df["MemPerCore"], df["CoreH_cumsum"])
    ax.set_xlabel("Memory in GB / Core")
    ax.set_ylabel("corehours (cumulative sum)")

    ax.set_xlim(left=-0.5, right=16)

    # vertical reference lines
    for x, label in zip((2, 4, 8), ("2 GB", "4 GB", "8 GB")):
        ax.axvline(x=x, color="gray", linestyle="--")
        ax.text(x, ax.get_ylim()[1]*0.95, label, rotation=90, va="top")

    fig.savefig(filename, dpi=150, bbox_inches="tight")
    plt.close()


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)10s:  %(message)s", level=logging.INFO) #logging.DEBUG)

    # Get data for Jan 2026:
    df, period = readDataFrame("2026-01-01", "2026-01-31")
    # If you want to filter on Accounts:
    #df = df.filter(~pl.col("Account").str.contains(r"^.*_gpu$"))
    print(df.tail())
    print(len(df))
    plot_cumsum(df, "2026_all.png")
    dfs = {acc: sub for acc, sub in df.group_by("Account")}
    for key in dfs.keys():
        accountname = key[0]
        plot_cumsum(dfs[key], f"Memory_2026_{accountname}.png")

    # Plot data for a range of years
    for year in [2025, 2024, 2023, 2022]:
        df, period = readDataFrame(f"{year}-01-01", f"{year}-12-31")
        plot_cumsum(df, f"{year}_all.png")
        dfs = {acc: sub for acc, sub in df.group_by("Account")}
        for key in dfs.keys():
            accountname = key[0]
            plot_cumsum(dfs[key], f"{year}_{accountname}.png")

    # Uncomment for interactive exploration:
    # import code
    # code.interact(local=locals())
