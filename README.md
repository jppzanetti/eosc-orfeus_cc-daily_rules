# eosc-orfeus_cc-daily_rules
## ingestion data and extract metadata from ORFEUS_CC daily files, apply some rules & actions on data
Composed by WFmetadata extraction, DublinCore compute, and iRODS/B2safe ingestion & management.

The project give the capability to choose one sequence of action to be applied to the data files (mseed) and execute this job every time that we want,  e.g. into crontab.

Like  WFCatalogcollector script this project elaborate the actions (rules) and apply they to a list of files that fall into specific requirement (date, update, etc..) that rely on very well done collector.

A separate file called ruleMap.json is in charge to set the sequence and what steps are involved, a first look to the file will be better than a lot of words for describe it.

All the metadata extracted here are inserted into mongoDB instance that is the same of the WFCollector (wfrepo) and the Dublin Core collection (useful for HTTP-API-B2STAGE) is integrated into this, iRODS integration is also performed in order to be able to expose our data on EUDAT/EOSC-HUB/EPOS ecosystems and to make our data more be FAIR.

- Generation of PIDs and save info inside iRODS and MongoDB; 
- make a EUDAT replication;
- WFCatalog Metadata extraction;
- and many other activities;

 are executed on regular base thanks to the iRODS rules and specific functions.

At this time we have a few rules and some actions but, in the future we can think about increase or change they, following the ORFEUS_CC nodes needs/policy. 

### Configuration

The configuration is done in two JSON files: config.json and ruleMap.json.

The first one, config.json, contains the configurations for managing policies, for example: MongoDB and iRODS connection configurations, Dublin Core definitions, log file name, and filters.

The second file, ruleMap.json, tells wfsequencer.py what is the workflow to be applied to each file. It defines:
1) which rules are available, in `_list_available_rules_`,
1) the mapping between rules and methods in wfsequencer.py, in `RULE_MAP`,
1) the sequence in which the rules are called, in `SEQUENCE`, and
1) the locations of external iRODS rules in disk, in `RULE_PATHS`.

### How to call the policy manager script
```
wfcatalog.py [-h] [--config] [--version]
             [--dir DIR] [--file FILE] [--list LIST]
             [--past {day,yesterday,week,fortnight,month}]
             [--date DATE] [--range RANGE]
             [--flags] [--csegs] [--hourly]
	     [--logfile LOGFILE] [--rulemap MAPFILE]
             [--update] [--force] [--delete] [--dc_on]
```

Arguments:
* `--dir`, `--file`, `--list`, `--past`, `--date` Define the files to be processed. Exactly one of these options need to be used to call the script:
  * `--dir DIR` Point to a directory containing the files to process.
  * `--file FILE` Choose a specific file to be processed.
  * `--list LIST` Specific list of files to be processed (e.g., `["file1", "file2"]`).
  * `--past {day,yesterday,week,fortnight,month}` Process files in a specific range in the past.
  * `--date DATE` Process files for a specific date.
  * `--range RANGE` A number of days after a specific date given by `--date`, default 1.

Help options:
* `-h`, `--help`, `--config`, `--version` Show the script help, configuration, or version and exits.

Optional arguments:
* `--csegs` Include continuous segments in result.
* `--hourly` Include hourly granules in result.
* `--logfile LOGFILE` Set custom logfile.
* `--rulemap MAPFILE` Set custom rule map.
* `--update` Update existing documents in the database.
* `--force` Force file updates.
* `--delete` Delete files from database.
* `--dc_on` Extract Dublin Core metadata for `do_wf` collection.

##### Usage example
```
wfcatalog.py --dir /data/SDS/ --dc_on
```
