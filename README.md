# cmd-weekly-deaths

Transform for the weekly deaths datasets
- Weekly deaths by local authority
- Weekly deaths by health board
- Weekly deaths by age & sex
- Weekly deaths by region

Transform takes in 2 input spreadsheets, one file name contains 'published' and one contains 'lahb' -> this is how the transform distinguishes the files
The spreadsheets are either provided by the business area or can be obtained from the ONS website 
https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/causesofdeath/datasets/deathregistrationsandoccurrencesbylocalauthorityandhealthboard
https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/datasets/weeklyprovisionalfiguresondeathsregisteredinenglandandwales
Place the input files in the same location as the notebook

4 output files are created
- v4-weekly-deaths-age-sex.csv 
- v4-weekly-deaths-regional.csv 
- v4-weekly-deaths-health-board-{year}.csv -> depends on year of data
- v4-weekly-deaths-local-authority-{year}.csv

WFunctions.py contains all the separate transform functions and the accompanying post processing functions
transform.ipynb runs the transform

Transform requires the use of databaker & databakerUtils
