# IB_historical_data_downloader
Original source came from https://gist.github.com/robcarver17/f50aeebc2ecd084f818706d9f05c1eb4

I made fairly extensive changes to make this run unattended, retrieve a whole bunch of one-minute bars for
multiple symbols over a large date range, and store the bars in appropriately named CSV files
while never violating IB's pacing restrictions.

It still needs more work to be more generalized, but it is getting the job done for my needs (feeding TensorFlow).
Be aware, a year of bars for two symbols can take about 24 hours.
