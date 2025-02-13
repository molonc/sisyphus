import pyranges as pr
import pandas as pd
import numpy as np
from io import StringIO

from dbclients.tantalus import TantalusApi

def upload_data_to_azure(tantalus_api, data, storage_name, blobname):
	"""
	Args:
		tantalus_api (obj): Tantalus API instance
		data (str): data to write to Azure
		storage_name (str): storage name registered in Tantalus
	"""
	storage_client = tantalus_api.get_storage_client(storage_name)
	storage_client.write_data_raw(blobname, data)

def rle(seq):
	"""
	run length encoding

	Args:
		seq (list): sequence to analyze. Will be a list of int?
	"""
	if not (seq):
		return

	result = {
		'lengths': [],
		'values': [],
	}
	count = 0
	prev = 0
	curr = 0
	while curr < len(seq):
		# Deal with NaN and None type
		if(all([pd.isna(seq[curr]), pd.isna(seq[prev])])):
			result['lengths'].append(1)
			result['values'].append(seq[curr])

			# move to the next value
			prev = curr
			count = 0
			curr += 1
		elif(seq[curr] == seq[prev]):
			count += 1
			curr += 1
		else:
			if(count != 0):
				result['lengths'].append(count)
				result['values'].append(seq[prev])

			prev = curr
			count = 0

	if(count != 0):
		result['lengths'].append(count)
		result['values'].append(seq[prev])

	return result

def get_human_chr_category():
	categories = []
	for i in range(1,23):
		categories.append(str(i))

	categories.extend(['X', 'Y'])

	return categories
	
def reads_to_segs(df):
	"""
	Daniel Lai's R script in Python implementation.
	"""
	longseg = df[["chr", "start", "end", "state", "copy", "multiplier", "cell_id"]]
	longrle = rle(list(df['state'].values))
	longseg['rle'] = np.repeat([i for i in range(0, len(longrle['lengths']))] ,longrle['lengths'])

	medseg = longseg.groupby(["chr", "state", "cell_id", "rle"]).agg({
		'start': 'min',
		'end': 'max',
		'copy': 'median',
		'multiplier': pd.unique,
	})

	shortseg = medseg.reset_index()
	shortseg = shortseg.rename(columns={'copy': 'median'})
	shortseg = shortseg[["chr", "start", "end", "state", "median", "multiplier", "cell_id"]]
	
	# change data type
	shortseg = change_to_bccrc_column_types(shortseg)
	shortseg = shortseg.sort_values(by=['cell_id', 'chr', 'start', 'end'])

	return shortseg

def prepare_blacklist_df(blacklist_file):
	blacklist_df = pd.read_csv(blacklist_file, sep='\t')

	renamed_blacklist_df = rename_to_pyrange_compatible_columns(blacklist_df)

	return renamed_blacklist_df

def prepare_reads_df(reads_file):
	reads_df = pd.read_csv(reads_file)
	# add ID column to keep track of unique ids
	reads_df['id'] = reads_df.index

	renamed_reads_df = rename_to_pyrange_compatible_columns(reads_df)

	return renamed_reads_df

def change_to_bccrc_column_types(df):
	chr_categories = get_human_chr_category()
	df['chr'] = pd.Categorical(df['chr'], categories=chr_categories, ordered=True)
	df['start'] = df['start'].astype('Int64')
	df['end'] = df['end'].astype('Int64')
	df['state'] = df['state'].astype('Int64')

	return df

def rename_to_pyrange_compatible_columns(df):
	cols_to_rename = {
		'seqnames': 'Chromosome',
		'chr': 'Chromosome',
		'start': 'Start',
		'end': 'End',
	}

	renamed_df = df.rename(columns=cols_to_rename)

	return renamed_df

def rename_to_bccrc_compatible_columns(df):
	cols_to_rename = {
		'Chromosome': 'chr',
		'Start': 'start',
		'End': 'end',
	}

	renamed_df = df.rename(columns=cols_to_rename)

	return renamed_df

def filter_reads(reads_file, blacklist_file):
	reads_df = prepare_reads_df(reads_file)
	blacklist_df = prepare_blacklist_df(blacklist_file)

	r1 = pr.PyRanges(reads_df)
	r2 = pr.PyRanges(blacklist_df)

	overlaps = r1.overlap(r2)
	overlaps_ids = overlaps.as_df()['id']

	# mimic readsToSegs
	masked_df = rename_to_bccrc_compatible_columns(reads_df)
	masked_df.loc[ masked_df['id'].isin(overlaps_ids), 'state' ] = -1
	cell_ids = pd.unique(masked_df['cell_id'])

	msegs = []
	for cell_id in cell_ids:
		msegs.append(reads_to_segs(masked_df[ masked_df['cell_id'] == cell_id ]))

	mseg = pd.concat(msegs, ignore_index=True)

	# Drop rows with state == NA
	masked_df = change_to_bccrc_column_types(masked_df)
	filtered_mseg_df = mseg[ ~pd.isna(mseg['state']) ]
	filtered_masked_df = masked_df[ ~pd.isna(masked_df['state']) ]

	# Drop rows with state < 0 ?
	#filtered_mseg_df = mseg[ mseg['state'] >= 0 ]
	#filtered_masked_df = masked_df[ masked_df['state'] >= 0 ]

	# drop id column
	filtered_masked_df = filtered_masked_df.drop(columns=['id'])

	return (filtered_masked_df, filtered_mseg_df)

if __name__ == '__main__':
	tantalus_api = TantalusApi()
	reads_file = '/home/dmin/A118429A_reads.csv'
	blacklist_file = '/home/dmin/blacklist_2018.10.23.txt'

	filtered_masked_df, filtered_mseg_df = filter_reads(reads_file, blacklist_file)

	filtered_mseg_out = filtered_mseg_df.to_csv(index=False, encoding='utf-8')
	filtered_masked_out = filtered_masked_df.to_csv(index=False, encoding="utf-8")

	upload_data_to_azure(tantalus_api, filtered_mseg_out, "singlecellresults_staging", "filtered_mseg.csv")
	upload_data_to_azure(tantalus_api, filtered_masked_out, "singlecellresults_staging", "filtered_masked.csv")