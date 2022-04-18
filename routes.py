from flask import Flask
import vaex as vx
import json
import gc
app = Flask(__name__)

def get_price_range(row):
    return [row['price_per_meter_min'],row['price_per_meter_max']]

def get_dist(flat):
    try:
        dfg = flat.groupby(['rooms_count'], agg={vx.agg.mean('price_per_meter'),vx.agg.min('price_per_meter'),vx.agg.max('price_per_meter'),vx.agg.count('price_per_meter')})
        dfg.rename('price_per_meter_mean','median')
        dfg.rename('price_per_meter_count','count')
        result = dfg.to_pandas_df()
        result['price_range'] = result.apply(get_price_range,axis=1)
        result = result.drop(['price_per_meter_min','price_per_meter_max'],axis = 1)
        result['median'] = round(result['median'])
        result = result.to_dict('records')
        gc.collect()
        return result
    except:
        return []

def filter_only_locaton_for_live(posted_data):
	try:
		data = vx.open('test.hdf5')
		d = data[data['special'] == '0']
		
		v = posted_data['city'].split('/')
		
		ch = "(d['nom_commune'] == \"" + str(v[0]) + "\") & (d['code_departement'] == '" + str(v[1]) + "')  |"
		ch = ch[:-1]
		d = d[eval(ch)]
		d = d[(d['status'] == 2) | (d['status'] == 0) & (d['count'] == 1)]
		return d
	except Exception as e:
		print(e)
		return {'error': True}

@app.route("/test", methods=["get"])
def test():
	try:
		posted_data = '{"area": 20,"cellar": 1,"city": "Toulouse/31/31000","code_insee": "06088","district": "","elevator": 0,"epoque": "After 1990","estate_type": "Appartement","floor": 1,"furnished": 0,"garden": 0,"isFurnished": 0,"location": "81 rue belliard 75018 Paris","nature": "Old","parking": 1,"postal_code": "49800","rooms_count": 3,"transaction_type": 2,"travaux": "Fully refurbished"}'
		posted_data = json.loads(posted_data)
		df = filter_only_locaton_for_live(posted_data)
		if type(df) == dict and 'error' in df and df['error']==True:
			return df

		df = df[df['price_per_meter'].isna() == False]
		df = df[df['rooms_count'].isna() == False]
		results = {}
		house_new = df[(df['estate_type'] == 'Maison / Villa') & (df['nature'] == 'New')]
		flat_new = df[(df['estate_type'] == 'Appartement') & (df['nature'] == 'New')]
		house_old = df[(df['estate_type'] == 'Maison / Villa') & (df['nature'] == 'Old')]
		flat_old = df[(df['estate_type'] == 'Appartement') & (df['nature'] == 'Old')]
		flat_new['rooms_count'] = flat_new['rooms_count'].apply(lambda x: 5 if x >= 5 else x)
		flat_old['rooms_count'] = flat_old['rooms_count'].apply(lambda x: 5 if x >= 5 else x)
		house_old['rooms_count'] = house_old['rooms_count'].apply(lambda x: 7 if x >= 7 else x)
		house_old['rooms_count'] = house_old['rooms_count'].apply(lambda x: 2 if x <= 2 else x)
		house_new['rooms_count'] = house_new['rooms_count'].apply(lambda x: 7 if x >= 7 else x)
		house_new['rooms_count'] = house_new['rooms_count'].apply(lambda x: 2 if x <= 2 else x)
		house_new = house_new[['price_per_meter','rooms_count']]
		house_old = house_old[['price_per_meter','rooms_count']]
		flat_old = flat_old[['price_per_meter','rooms_count']]
		flat_new = flat_new[['price_per_meter','rooms_count']]

		results['apartment'] = {}
		results['villa'] = {}
		if len(flat_new) != 0:
			results['apartment']['New'] = get_dist(flat_new)
		else:
			results['apartment']['New'] = []
		
		if len(flat_old) != 0:
			results['apartment']['Old'] = get_dist(flat_old)
		else:
			results['apartment']['Old'] = []
			
		if len(house_new) != 0:
			results['villa']['New'] = get_dist(house_new)
		else:
			results['villa']['New'] = []
			
		if len(house_old) != 0:
			results['villa']['Old'] = get_dist(house_old)
		else:
			results['villa']['Old'] = []
		del flat_new,flat_old,house_old,house_new
		gc.collect()
		return results
	except Exception as e:
		return {'error': True}

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5050, debug=True)
