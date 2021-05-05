No files are shown when running `stat` in a non-existent directory:

  $ ../index_fsck.exe stat ../data/non-existent-store
  >> Getting statistics for store: `../data/non-existent-store'
  {
    "entry_size": "40.0 B",
    "files": {}
  }

Running `stat` on an index after 10 merges:

  $ ../index_fsck.exe stat ../data/random > log.txt 2>&1
  $ sed -Ee 's/"lock": "[0-9]+"/"lock": "<PID>"/g' log.txt
  >> Getting statistics for store: `../data/random'
  {
    "entry_size": "40.0 B",
    "files": {
      "data": {
        "size": "35.6 KiB",
        "offset": 36360,
        "generation": 9,
        "fanout_size": "128.0 B"
      },
      "log": {
        "size": "3.6 KiB",
        "offset": 3680,
        "generation": 9,
        "fanout_size": "0.0 B"
      },
      "lock": "<PID>"
    }
  }
