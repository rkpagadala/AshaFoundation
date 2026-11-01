# Data Notice

## Sources

- **Project funding and status data** (`DataCSV/`) is derived from publicly available
  project pages on [ashanet.org](https://ashanet.org/), © Asha for Education. It was
  collected from public pages (permitted by the site's `robots.txt`) and reduced to
  factual, non-personal fields: project ID, status label, stewarding chapter, Indian
  state, year, currency and amount.
- **Population data** (`DataCSV/population.csv`) is from the Census of India (2011)
  and published 2023 estimates.

## What is deliberately excluded

Project pages on ashanet.org list contact details for partner organisations. None of
that is stored here. The extraction pipeline (`Download/extraction.py`) drops project
steward, project partner, other contacts, project address and telephone fields before
writing any CSV; the address is used only transiently to derive the state. Raw
downloaded HTML (`Download/HTML_DATA/`) is git-ignored and must not be committed.

## Licensing

The **code** in this repository is released under the MIT License (see `LICENSE`).
The **data** in `DataCSV/` is not covered by that license: the underlying facts belong
to their original publishers and are redistributed here solely for analysis.

## Affiliation

This is an independent analysis. It is not an official publication of, and is not
endorsed by, Asha for Education. Funding figures are as self-reported on ashanet.org;
all derived statistics and any errors are the authors' own.

If you are a rights-holder or an individual with a concern about any content here,
please open an issue and it will be addressed promptly.
