#  🎈 feedparq

[feedparq.club](https://feedparq.club) is a toy rss feed aggregator
that renders items in hackernews style - showing only the top K items.

Each file in the [`feedparq-rs/config`](./feedparq-rs/config/) directory 
is a list of rss/atom feeds that are aggregated and displayed on
[feedparq.club](https://feedparq.club).

e.g. the `feedparq-rs/config/news` file lists the feeds that are 
aggregated at [feedparq.club/news](https://feedparq.club/news)


## Contributing 

Yes please help curate good feeds!

* add a file with the URL of an rss/atom feed on each line
* name it what the slug should be
* ( or add to the existing files )
* update the cloudflare function and slugs.json in `feedparq-club/functions`
* Submit a PR with you changes

When your PR is merged, your changes will appear on [feedparq.club](https://feedparq.club)
(after some days while I deploy the things).
