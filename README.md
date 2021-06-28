# Estimating Earnings Response Coefficient
This repo is used to estimate pooled and firm-specific ERC. To generate output, first, run the r code to download data from WRDS. This code make use of a .pgpass file to login to WRDS, please add your username to connect. Then run data\_cleaning.py in py and erc\_analysis.py in py\_code foler.
Once you've done that, the graphs and table will be generated in a folder called output.
To generate the beamer, I use org-beamer-export-to-pdf in EMACS to generate the slides using erc\_preso.org in the doc folder. This function create a tex file and a pdf file within the same folder.
Please note that the entire workflow is within EMACS, but you can run R, Python separately then create the beamer using EMACS. 
