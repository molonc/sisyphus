"""Contains some useful constants for the scripts."""


# Logging stuff
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

REF_GENOME_MAP = {
    'HG18': [r'hg[-_ ]?18',                
             r'ncbi[-_ ]?build[-_ ]?36.1',  
            ],
    'HG19': [r'hg[-_ ]?19',                
             r'grc[-_ ]?h[-_ ]?37',        
             r'ncbi[-_ ]?build[-_ ]?37'
            ],}