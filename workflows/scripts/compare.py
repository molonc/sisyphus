import pandas as pd

def compare_segments():
	ref = "/projects/molonc/aparicio_lab/dmin/filter/A118429A_segments_filtered.csv"
	obs = "/projects/molonc/aparicio_lab/dmin/filter/test_segments_filtered.csv"

	ref_df = pd.read_csv(ref)
	obs_df = pd.read_csv(obs)

	# make sure all the column values are the same or similar enough
	threshold = 0.001

	ref_df = ref_df.fillna(0)
	obs_df = obs_df.fillna(0)

	cell_id_equal = ref_df['cell_id'].equals(obs_df['cell_id'])
	chr_equal = ref_df['chr'].equals(obs_df['chr'])
	start_equal = ref_df['start'].equals(obs_df['start'])
	end_equal = ref_df['end'].equals(obs_df['end'])
	state_equal = ref_df['state'].equals(obs_df['state'])
	multiplier_equal = ref_df['multiplier'].equals(obs_df['multiplier'])

	median_diff = abs(ref_df['median'] - obs_df['median'])
	median_equal = all(median_diff <= threshold)

	assert(cell_id_equal == True)
	assert(chr_equal == True)
	assert(start_equal == True)
	assert(end_equal == True)
	assert(state_equal == True)
	assert(multiplier_equal == True)
	assert(median_equal == True)

def compare_reads():
	ref = "/projects/molonc/aparicio_lab/dmin/filter/A118429A_reads_filtered.csv"
	obs = "/projects/molonc/aparicio_lab/dmin/filter/test_reads_filtered.csv"

	ref_df = pd.read_csv(ref)
	obs_df = pd.read_csv(obs)

	threshold = 0.001
	ref_df = ref_df.fillna(0)
	obs_df = obs_df.fillna(0)

	cell_id_equal = ref_df['cell_id'].equals(obs_df['cell_id'])
	chr_equal = ref_df['chr'].equals(obs_df['chr'])
	start_equal = ref_df['start'].equals(obs_df['start'])
	end_equal = ref_df['end'].equals(obs_df['end'])
	multiplier_equal = ref_df['multiplier'].equals(obs_df['multiplier'])
	modal_quantile_equal = ref_df['modal_quantile'].equals(obs_df['modal_quantile'])
	state_equal = ref_df['state'].equals(obs_df['state'])
	valid_equal = ref_df['valid'].equals(obs_df['valid'])
	ideal_equal = ref_df['ideal'].equals(obs_df['ideal'])

	copy_diff = abs(ref_df['copy'] - obs_df['copy'])
	modal_curve_diff = abs(ref_df['modal_curve'] - obs_df['modal_curve'])
	cor_gc_diff = abs(ref_df['cor_gc'] - obs_df['cor_gc'])
	copy_equal = all(copy_diff <= threshold)
	modal_curve_equal = all(modal_curve_diff <= threshold)
	cor_gc_equal = all(cor_gc_diff <= threshold)

	assert(cell_id_equal == True)
	assert(chr_equal == True)
	assert(start_equal == True)
	assert(end_equal == True)
	assert(state_equal == True)
	assert(multiplier_equal == True)
	assert(copy_equal == True)

	assert(modal_quantile_equal == True)
	assert(valid_equal == True)
	assert(ideal_equal == True)
	assert(modal_curve_equal == True)
	assert(cor_gc_equal == True)

compare_reads()
compare_segments()