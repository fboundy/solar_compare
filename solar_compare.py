#%%
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from datetime import time
import matplotlib.pyplot as plt
import yaml

start_date = "2023-01-01"
interval = "7D"

# Import: flux, eco7, agile, cosy, go
# Export: flux: fixed, seg, agile

options = [
    {
        "import": "flux", 
        "export": "flux",
        "color": "red",
    },

    {
        "import": "eco7", 
        "export": "agile",
        "color": "black",
    },
    {
        "import": "eco7", 
        "export": "fix",
        "color": "blue",
    },    
    {
        "import": "agile", 
        "export": "fix",
        "color": "cyan",
    }
]

with open('secrets.yaml', 'r') as file:
    secrets = yaml.safe_load(file)

import_url = f"https://api.octopus.energy/v1/electricity-meter-points/{secrets['octopus_import_mpan']}/meters/{secrets['octopus_serial']}/consumption/"
export_url = f"https://api.octopus.energy/v1/electricity-meter-points/{secrets['octopus_export_mpan']}/meters/{secrets['octopus_serial']}/consumption/"
# %%
params = {
    "page_size": 25000,
    "period_from": datetime(year= pd.Timestamp(start_date).year, month= pd.Timestamp(start_date).month, day= pd.Timestamp(start_date).day),
    "order_by": "period",
}

r = requests.get(import_url, auth=(secrets["octopus_api_key"],''), params=params)
import_data = pd.DataFrame(r.json()['results']).set_index('interval_start')['consumption']
import_data.name="import"

r = requests.get(export_url, auth=(secrets["octopus_api_key"],''),  params=params)
export_data = pd.DataFrame(r.json()['results']).set_index('interval_start')['consumption']
export_data.name="export"

df = pd.DataFrame([import_data, export_data]).transpose().fillna(0)
df.index = pd.to_datetime(df.index, utc=True)
#%%
products_url = "https://api.octopus.energy/v1/products/"
r = requests.get(products_url)
DEBUG=True

print(r)
tariffs = ['import', 'export']

if r.status_code == 200:
    products = r.json()["results"]

    codes = {}
    codes["import"] = [
        p["code"]
        for p in products
        if not max(
            [
                x in p["code"]
                for x in [
                    "BULB",
                    "LP",
                    "BB",
                    "M-AND-S",
                    "AFFECT",
                    "COOP",
                    "OUTGOING",
                    "PREPAY",
                    "EXPORT",
                    "PP",
                    "ES",
                    "OCC",
                ]
            ]
        )
    ]

    codes["export"] = [
        p["code"]
        for p in products
        if max([x in p["code"] for x in ["OUTGOING", "EXPORT"]]) and not "BB" in p["code"]
    ]

    alt_codes = {
        "import": {
            "agile": "AGIL",
            "cosy": "COSY",
            "go": "GO-V",
            "eco7": "VAR-",
            "flux": "FLUX",
        },
        "export": {
            "agile": "AGILE-OUTG",
            "fix": "OUTGOING-F",
            "flux": "FLUX-EXPOR",
            "seg": "OUTGOING-S",
        },
    }

    product_codes = {
        "import": {},
        "export": {},
    }

    tariff_codes = {
        "import": {},
        "export": {},
    }

    rates = {
        "import": {},
        "export": {},
    }

    area_suffix = "-G"

    for t in tariffs:
        print(codes[t])
        for key in alt_codes[t].keys():
            product_codes[t][key] = [c for c in codes[t] if c[:len(alt_codes[t][key])]==alt_codes[t][key]][0]
            if key == "eco7":
                prefix = "E-2R-"
                rates[t][key] = ['standing-charges', 'day-unit-rates','night-unit-rates']
            else:
                prefix = "E-1R-"
                rates[t][key] = ['standing-charges', 'standard-unit-rates']
            
            if t == "export":
                rates[t][key] = rates[t][key][1:]

            tariff_codes[t][key] = prefix + product_codes[t][key]+area_suffix


# %%
# for t in tariffs:


for t in tariffs:
# for t in ["export"]:
    for k in product_codes[t].keys():
    # for k in ["flux"]:
        for rate in rates[t][k]:
            if "unit" in rate:
                rate2 = "unit"
            else:
                rate2 = "standing"
                    
            s = pd.Series()
            s.name= f"{t}_{k}_{rate2}"

            for m in range(pd.Timestamp.now().month):
                date_start = pd.Timestamp(f"2023-{m + 1:2d}-01")+pd.Timedelta(days=-1)
                if m == 11:
                    date_end = pd.Timestamp(f"2024-01-01") 
                else:
                    date_end = pd.Timestamp(f"2023-{m + 2:2d}-01")

                params = {
                    "page_size": 1500,
                    "period_from": date_start,
                    "period_to": date_end,
                    "order_by": "period",
                }

                url = f"{products_url}/{product_codes[t][k]}/electricity-tariffs/{tariff_codes[t][k]}/{rate}/"
                print("\t",rate, url)
                r = requests.get(url, params=params)
                if r.status_code==200 and len(r.json()['results']) > 0:
                    x=pd.DataFrame(r.json()['results']).set_index("valid_from").sort_index()['value_inc_vat']
                    x=x[np.invert(x.index.duplicated(keep='first'))]
                    if len(s) == 0: 
                        s=pd.concat([x,s])
                    elif x.index[0] != s.index[0]:
                        s=pd.concat([x,s])

                else:
                    print("Fail")

            s.index = pd.to_datetime(s.index, utc=True)
            s=pd.concat([s, pd.Series(index=[df.index[0]], data=[np.NaN])]) 
            s=s.sort_index()
            s=s.interpolate(method="ffill")

            s=s[np.invert(s.index.duplicated())]
            if k == "eco7" and rate2 == "unit":
                s=s.reindex(df.index).interpolate(method="ffill")
                if rate[:3] == 'day':
                    mask=(df.index.time<time(0,30)) | (df.index.time>time(7,30)) 
                else:
                    mask=(df.index.time>=time(0,30)) & (df.index.time<time(7,30)) 
                df.loc[mask, [f"{t}_{k}_{rate2}"]] = s[mask]
            else:
                df[f"{t}_{k}_{rate2}"] = s[s.index<=df.index[-1]]
                
df = df.interpolate(method="ffill")
        
# %%
# fix Flux which didn't exist before 14-02-2023
l=int(len(df[df['import_flux_standing'].isna()])  / 2)
for c in ['import_flux_standing', 'export_flux_unit', 'import_flux_unit']:
    df[c].iloc[:l]=df[c].iloc[2 * l:3 * l]
    df[c].iloc[l:2 * l]=df[c].iloc[2* l:3 * l]

# %%


for o in options:
    df[f"cost_{o['import']}_{o['export']}"] = (df['import'] * df[f"import_{o['import']}_unit"] + df[f"import_{o['import']}_standing"] / 48 - df['export'] * df[f"export_{o['export']}_unit"] )/ 100

fig, ax = plt.subplots(2,2, figsize=(12,8))
ax=ax.flatten()
start = pd.Timestamp("2023-01-01").tz_localize("UTC")
end = pd.Timestamp.now().tz_localize("UTC")

dfx = df.loc[(df.index >=start) & (df.index<=end)]

for o in options:
    print(o)
    (dfx[f"cost_{o['import']}_{o['export']}"].resample(interval).mean()*48).plot(ax=ax[0], color=o['color'], label=f"{o['import']}/{o['export']}")
    dfx[f"cost_{o['import']}_{o['export']}"].cumsum().plot(ax=ax[1], c=o['color'], label=f"{o['import']}/{o['export']}")
    mask = (dfx[f"cost_{o['import']}_{o['export']}"].resample(interval).mean() == dfx[[f"cost_{o['import']}_{o['export']}" for o in options]].resample(interval).mean().min(axis=1))
    ax[3].scatter(dfx["export"].resample(interval).mean()[mask]*48, dfx["import"].resample(interval).mean()[mask]*48, c=o['color'], label=f"{o['import']}/{o['export']}")

(dfx["import"].resample(interval).mean()*48).plot(ax=ax[2])
(dfx["export"].resample(interval).mean()*48).plot(ax=ax[2])    

ax[3].set_xlabel("Mean Daily Export (kWh)")
ax[0].set_ylabel("Mean Daily Net Cost (GBP)")
ax[1].set_ylabel("Cumulative Net Cost (GBP)")
ax[2].set_ylabel("Mean Daily Import/Export (kWh)")
ax[3].set_ylabel("Mean Daily Import (kWh)")
ax[0].legend()
ax[3].legend()
ax[2].legend()
ax[3].set_title("Lowest Cost Tariff")
for i in range(3):
    ax[i].set_xlabel(None)

    # ax[1].legend()
 # %%