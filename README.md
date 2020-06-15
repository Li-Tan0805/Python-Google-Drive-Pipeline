Automate the process of Google Drive files ingestion to Datorama with following steps:
1. Download: call Google Drive API to download targeted buy details
2. Combine: combine all Buy Details into one big data frame
3. Manupulate: clean up empty rows, generate placement name, diagnose problematic input using try except caluse
4. Email: send output data to Datorama and team via yagmail package
