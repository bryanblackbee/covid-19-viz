import json

import requests
import pandas as pd

def _ingest_json():
	resp = requests.get('https://pomber.github.io/covid19/timeseries.json')
	resp_json_str = resp.text
	resp_dict = json.loads(resp_json_str)

	l = []
	for c, cl in resp_dict.items():
		for d in cl:
			d.update({'country' : c})
			l.append(d)
	df = pd.DataFrame(l)

	return df

def load_single_country(country='') -> pd.DataFrame:
	## Ingest JSON endpoint
	df_universe = _ingest_json()
	df_c = df_universe[df_universe.country==country].copy()

	## Preprocessing
	df_c['date'] = pd.to_datetime(df_c['date'], format='%Y-%m-%d')
	df_c['date'] = df_c['date'].dt.strftime('%Y-%m-%d')
	df_c['date'] = df_c['date'].astype(str)

	## Feature Engineering
	# Feature: New cases
	df_c['confirmed_tm1'] = df_c['confirmed'].shift(1)
	df_c['confirmed_tm1'] = df_c['confirmed_tm1'].fillna(0)
	df_c['confirmed_tm1'] = df_c['confirmed_tm1'].astype(int)
	df_c['new'] = df_c['confirmed'] - df_c['confirmed_tm1']
	_ = df_c.drop(['confirmed_tm1'], axis=1, inplace=True)
	# Feature: Currently Hospitalised
	df_c['active'] = df_c['confirmed'] - df_c['recovered'] - df_c['deaths']

	## Output
	_ = df_c.rename(columns={'confirmed' : 'total',
	'recovered' : 'discharged'}, inplace=True)
	_ = df_c.reset_index(drop=True, inplace=True)
	# Follow Mothership's columns naming
	c_cols = ['country', 'date',
	'total', 'new', 'deaths', 'discharged', 'active']
	return df_c[c_cols]

def load_multiple_countries(countries=[]) -> pd.DataFrame:
	countries_dfs = []
	for i in countries:
		countries_dfs.append(load_single_country(i))
	df = pd.concat(countries_dfs).reset_index(drop=True)
	return df
